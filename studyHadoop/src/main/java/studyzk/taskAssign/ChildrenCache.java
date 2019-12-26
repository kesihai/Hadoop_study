package studyzk.taskAssign;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ChildrenCache {

  protected List<String> children;

  ChildrenCache() {
    this(new LinkedList<>());
  }

  ChildrenCache(List<String> children) {
    this.children = children;
  }

  List<String> getList() {
    return new LinkedList<>(children);
  }

  void updateList(List<String> children) {
    this.children = new LinkedList<>(children);
  }

  public List<String> getLostNode(List<String> newChildren) {
    List<String> lostNodes = new LinkedList<>();
    for (String node : children) {
      if (!newChildren.contains(node)) {
        lostNodes.add(node);
      }
    }
    return lostNodes;
  }

  public List<String> getAddNode(List<String> newChildren) {
    List<String> addNodes = new LinkedList<>();
    for (String node : newChildren) {
      if (!children.contains(node)) {
        addNodes.add(node);
      }
    }
    return addNodes;
  }
}
