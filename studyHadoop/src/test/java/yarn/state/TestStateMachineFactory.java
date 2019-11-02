package yarn.state;

import org.junit.Assert;
import org.junit.Test;

/**
 * test state machine.
 */
public class TestStateMachineFactory {

  enum DayStatus {
    MORNING,
    AFTERNOON,
    EVENING
  }

  enum FoodType {
    MORNING_FOOD,
    AFTERNOON_FOOD,
    EVENING_FOOD
  }

  private interface Food {

    FoodType getFoodType();

    void eat();
  }

  private class MorningFood implements Food {

    @Override
    public FoodType getFoodType() {
      return FoodType.MORNING_FOOD;
    }

    @Override
    public void eat() {
      System.out.println("I am going to eat Morning food");
    }
  }

  private class AfterNoonFood implements Food {

    @Override
    public FoodType getFoodType() {
      return FoodType.AFTERNOON_FOOD;
    }

    @Override
    public void eat() {
      System.out.println("I am going to eat afternoon food");
    }
  }

  private class EveningFood implements Food {

    @Override
    public FoodType getFoodType() {
      return FoodType.EVENING_FOOD;
    }

    @Override
    public void eat() {
      System.out.println("I am going to eat evening food");
    }
  }

  private class MorningTransition
      implements SingleArcTransition<TestStateMachineFactory, DayStatus, Food> {

    @Override
    public DayStatus doTransition(
        TestStateMachineFactory testStateMachineFactory, Food food) {
      System.out.println("I have finish morning food");
      return DayStatus.AFTERNOON;
    }
  }

  private class AfternoonTransition
      implements SingleArcTransition<TestStateMachineFactory, DayStatus, Food> {

    @Override
    public DayStatus doTransition(
        TestStateMachineFactory testStateMachineFactory, Food food) {
      System.out.println("I have finish afternoon food");
      return DayStatus.EVENING;
    }
  }

  private class EveningTranstion
      implements SingleArcTransition<TestStateMachineFactory, DayStatus, Food> {

    @Override
    public DayStatus doTransition(
        TestStateMachineFactory testStateMachineFactory, Food food) {
      System.out.println("I have finish evening food, and I want to sleep");
      return DayStatus.EVENING;
    }
  }

  @Test
  public void testStateMachine() {
    StateMachineFactory<TestStateMachineFactory, DayStatus, FoodType, Food>
        factory = new StateMachineFactory();
    factory
        .addTransition(
            DayStatus.MORNING, FoodType.MORNING_FOOD,
            new MorningTransition(), DayStatus.AFTERNOON)
        .addTransition(
            DayStatus.AFTERNOON, FoodType.AFTERNOON_FOOD,
            new AfternoonTransition(), DayStatus.EVENING)
        .addTransition(
            DayStatus.EVENING, FoodType.EVENING_FOOD,
            new EveningTranstion(), DayStatus.EVENING);
    StateMachine<DayStatus, FoodType, Food> stateMachine
        = factory.make(new TestStateMachineFactory(), DayStatus.MORNING);
    stateMachine.doTransition(FoodType.MORNING_FOOD, new MorningFood());
    stateMachine.doTransition(FoodType.AFTERNOON_FOOD, new AfterNoonFood());
    stateMachine.doTransition(FoodType.EVENING_FOOD, new EveningFood());
    Assert.assertEquals(DayStatus.EVENING, stateMachine.getCurrentState());
  }

}