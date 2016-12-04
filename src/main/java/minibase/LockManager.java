package minibase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Stack;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;


public class LockManager {

    private final ConcurrentHashMap<PageId, Object> locks;
    private final HashMap<PageId, ArrayList<TransactionId>> sharedLocks;
    private final HashMap<PageId, TransactionId> exclusiveLocks;
    private final ConcurrentHashMap<TransactionId, Collection<PageId>> transactionPages;
    private final ConcurrentHashMap<TransactionId, Collection<TransactionId>> dependencyGraph;

    private LockManager() {
        locks = new ConcurrentHashMap<PageId, Object>();
        sharedLocks = new HashMap<PageId, ArrayList<TransactionId>>();
        exclusiveLocks = new HashMap<PageId, TransactionId>();
        transactionPages = new ConcurrentHashMap<TransactionId, Collection<PageId>>();
        dependencyGraph = new ConcurrentHashMap<TransactionId, Collection<TransactionId>>();
    }

    public static LockManager getInstance() {
        return new LockManager();
    }

    private boolean hasPermissions(TransactionId transactionId, PageId pageId, Permissions permissions) {
        if (exclusiveLocks.containsKey(pageId) && transactionId.equals(exclusiveLocks.get(pageId))) {
            return true;
        }
        if (permissions == Permissions.READ_ONLY) {
            return sharedLocks.containsKey(pageId) && sharedLocks.get(pageId).contains(transactionId);
        }
        return false;
    }

    private Object getLock(PageId pageId) {
        locks.putIfAbsent(pageId, new Object());
        return locks.get(pageId);
    }

    private void addPageToTransactionPages(TransactionId transactionId,
                                           PageId pageId) {
        transactionPages.putIfAbsent(transactionId, new LinkedBlockingQueue<PageId>());
        transactionPages.get(transactionId).add(pageId);
    }

    public boolean acquireLock(TransactionId transactionId, PageId pageId, Permissions permissions) throws TransactionAbortedException {
        if (hasPermissions(transactionId, pageId, permissions)) {
            return true;
        }
        if (transactionId == null) {
            transactionId = TransactionId.NULL_TRANSACTION_ID;
        }
        Object lock = getLock(pageId);
        if (permissions == Permissions.READ_ONLY) {
            while (true) {
                synchronized (lock) {
                    TransactionId exclusiveLockHolder = exclusiveLocks.get(pageId);
                    if (exclusiveLockHolder == null || transactionId.equals(exclusiveLockHolder)) {
                        removeDependencies(transactionId);
                        sharedLocks.put(pageId, new ArrayList<TransactionId>());
                        sharedLocks.get(pageId).add(transactionId);
                        addPageToTransactionPages(transactionId, pageId);
                        return true;
                    }
                    addDependency(transactionId, exclusiveLockHolder);
                }
            }
        } else {
            while (true) {
                synchronized (lock) {
                    ArrayList<TransactionId> lockHolders = new ArrayList<TransactionId>();
                    if (exclusiveLocks.containsKey(pageId)) {
                        lockHolders.add(exclusiveLocks.get(pageId));
                    } else if (sharedLocks.containsKey(pageId)) {
                        lockHolders.addAll(sharedLocks.get(pageId));
                    }
                    if (lockHolders.isEmpty() || (lockHolders.size() == 1 && transactionId.equals(lockHolders.iterator().next()))) {
                        exclusiveLocks.put(pageId, transactionId);
                        addPageToTransactionPages(transactionId, pageId);
                        return true;
                    }
                    addDependencies(transactionId, lockHolders);
                }
            }
        }
    }

    private void releaseLock(TransactionId transactionId, PageId pageId) {
        Object lock = getLock(pageId);
        synchronized (lock) {
            exclusiveLocks.remove(pageId);
            if (sharedLocks.containsKey(pageId)) {
                sharedLocks.get(pageId).remove(transactionId);
            }
        }
    }

    public void releasePage(TransactionId transactionId, PageId pageId) {
        releaseLock(transactionId, pageId);
        if (transactionPages.containsKey(transactionId)) {
            transactionPages.get(transactionId).remove(pageId);
        }
    }

    public void releasePages(TransactionId transactionId) {
        if (transactionPages.containsKey(transactionId)) {
            Collection<PageId> pageIds = transactionPages.get(transactionId);
            for (PageId pageId : pageIds) {
                releaseLock(transactionId, pageId);
            }
            transactionPages.remove(transactionId);
        }
    }

    public boolean holdsLock(TransactionId transactionId, PageId pageId) {
        Object lock = getLock(pageId);
        synchronized (lock) {
            if (exclusiveLocks.containsKey(pageId) && exclusiveLocks.get(pageId).equals(transactionId)) {
                return true;
            }
            if (sharedLocks.containsKey(pageId) && sharedLocks.get(pageId).contains(transactionId)) {
                return true;
            }
        }
        return false;
    }

    private void checkCycle(TransactionId transactionId, HashSet<TransactionId> testedTransactionIds, Stack<TransactionId> parents) throws TransactionAbortedException {
        testedTransactionIds.add(transactionId);
        if (dependencyGraph.containsKey(transactionId)) {
            for (TransactionId dependee : dependencyGraph.get(transactionId)) {
                if (parents.contains(dependee)) {
                    throw new TransactionAbortedException();
                }
                if (testedTransactionIds.contains(dependee)) {
                    continue;
                }
                parents.push(transactionId);
                checkCycle(dependee, testedTransactionIds, parents);
                parents.pop();
            }
        }
    }

    private void checkDeadLock() throws TransactionAbortedException {
        HashSet<TransactionId> testedTransactionIds = new HashSet<TransactionId>();
        for (TransactionId transactionId : dependencyGraph.keySet()) {
            if (testedTransactionIds.contains(transactionId)) {
                continue;
            }
            checkCycle(transactionId, testedTransactionIds, new Stack<TransactionId>());
        }
    }

    private void addDependency(TransactionId dependent, TransactionId dependee) throws TransactionAbortedException {
        Collection<TransactionId> dependees = new ArrayList<TransactionId>();
        dependees.add(dependee);
        addDependencies(dependent, dependees);
    }

    private void addDependencies(TransactionId dependent, Collection<TransactionId> dependees) throws TransactionAbortedException  {
        dependencyGraph.putIfAbsent(dependent, new LinkedBlockingQueue<TransactionId>());
        Collection<TransactionId> dependeesCollection = dependencyGraph.get(dependent);
        boolean needCheck = false;
        for (TransactionId dependee : dependees) {
            if (!dependencyGraph.get(dependent).contains(dependee) && !dependee.equals(dependent)) {
                needCheck = true;
                dependeesCollection.add(dependee);
            }
        }
        if (needCheck) {
            checkDeadLock();
        }
    }


    private void removeDependencies(TransactionId transactionId) {
        dependencyGraph.remove(transactionId);
    }
}