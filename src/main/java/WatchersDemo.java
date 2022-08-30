import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class WatchersDemo implements Watcher {

  private static final String ZOOKEEPER_HOST = "localhost:2181";
  private static final int SESSION_TIMEOUT_MS = 3000;

  private static final String TARGET_ZNODE_TO_BE_WATCHED = "/target_znode_to_be_watched";

  private ZooKeeper zooKeeper;

  public static void main(String[] main) throws IOException, InterruptedException {

    WatchersDemo watchersDemo = new WatchersDemo();
    watchersDemo.connectToZooKeeperServer();

    watchersDemo.run();
    watchersDemo.close();
  }

  /**
   * This implementation is just for example purpose. Ideally watchers for different purpose should
   * be defined separately instead of having one method with a switch block handling all the events
   * that ever occur in the application.
   * @param event
   */
  @Override
  public void process(WatchedEvent event) {
    switch (event.getType()){
      case None:
        if (event.getState().equals(KeeperState.SyncConnected)) {
          System.out.println("Successful connection event to ZooKeeper server received! Current Thread ID: " + Thread.currentThread().getId());
        } else {
          System.out.println("Zookeeper Disconnection event to Zookeeper server received! Current Thread ID: " + Thread.currentThread().getId());
          synchronized (zooKeeper) {
            zooKeeper.notifyAll();
          }
        }
        break;
      case NodeDeleted:
        System.out.println("Target Znode: " + TARGET_ZNODE_TO_BE_WATCHED + " deleted");
        break;
      case NodeCreated:
        System.out.println("Target Znode: " + TARGET_ZNODE_TO_BE_WATCHED + " created");
        break;
      case NodeDataChanged:
        System.out.println("Target Znode: " + TARGET_ZNODE_TO_BE_WATCHED + " data changed");
        break;
      case NodeChildrenChanged:
        System.out.println("Target Znode: " + TARGET_ZNODE_TO_BE_WATCHED + " children changed");
        break;
    }

    try {
      printCurrentTargetZnodeInfoAndRegisterWatchers();
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public void printCurrentTargetZnodeInfoAndRegisterWatchers() throws InterruptedException, KeeperException {

    Optional<Stat> statOptional = Optional.ofNullable(zooKeeper.exists(TARGET_ZNODE_TO_BE_WATCHED, this));
    if (statOptional.isPresent()) {
      Stat stat = statOptional.get();
      byte[] data = zooKeeper.getData(TARGET_ZNODE_TO_BE_WATCHED, this, stat);

      List<String> children = zooKeeper.getChildren(TARGET_ZNODE_TO_BE_WATCHED, this);

      System.out.println("Target Znode data: " + Arrays.toString(data) + ". Children of Target Znode: " + children);
    }
  }

  public void connectToZooKeeperServer() throws IOException {
    zooKeeper = new ZooKeeper(ZOOKEEPER_HOST, SESSION_TIMEOUT_MS, this);
  }

  private void run() throws InterruptedException {
    synchronized (zooKeeper) {
      zooKeeper.wait();
    }
  }

  public void close() throws InterruptedException {
    zooKeeper.close();
  }
}
