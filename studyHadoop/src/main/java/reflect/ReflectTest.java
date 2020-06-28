package reflect;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

@Data
class A {
  int x;
  int y;
  Hello h = Hello.F;
  List<Hello> list = Arrays.asList(Hello.F, Hello.FF);
}

enum Hello {
  F,
  FF,
  FFF
}

@Slf4j
public class ReflectTest {
  public static void main(String[] args) throws IllegalAccessException {
    A a = new A();
    Field[] fields = A.class.getDeclaredFields();
    for (Field field : fields) {
      if (field.getType() == int.class) {
        System.out.println("int: " + field.get(a));
      } else if (field.getType() == Hello.class) {
        System.out.println("Hello:" + field.get(a));
      } else if (field.getType() == List.class) {
        System.out.println("list:" + field.get(a));
      }
    }
    found(Arrays.asList(Hello.F, Hello.FF), Arrays.asList(Hello.FF, Hello.FFF));
  }

  public static <T> void found(
      List<T> first,
      List<T> second) {
    List<T> ff = new LinkedList<>();
    List<T> ss = new LinkedList<>();
    for (T t : first) {
      if (!second.contains(t)) {
        ff.add(t);
      }
    }

    for (T t : second) {
      if (!first.contains(t)) {
        ss.add(t);
      }
    }
    log.info(ff.toString());
    log.info(ss.toString());
  }
}
