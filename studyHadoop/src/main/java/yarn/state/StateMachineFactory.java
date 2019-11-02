package yarn.state;

import java.util.HashMap;
import java.util.Map;

/**
 * StateMachine factory.
 */
public class StateMachineFactory<
    OPERAND,
    STATE extends Enum<STATE>,
    EVENTTYPE extends Enum<EVENTTYPE>,
    EVENT> {

  Map<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENT>>> mp =
      new HashMap<>();
  TransitionList transitionList;
  OPERAND operand;
  STATE curState;

  // =============== function of StateMachineFactory ==============
  public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(
      STATE oldState,
      EVENTTYPE type,
      SingleArcTransition<OPERAND, STATE, EVENT> transition,
      STATE postState) {
    this.transitionList = new TransitionList(
        new SingleArcApplicableTransition(oldState, type,
            new SingleArcInternal(postState, transition)),
        this.transitionList);
    return this;
  }

  public StateMachine<STATE, EVENTTYPE, EVENT> make(
      OPERAND operand, STATE state) {
    this.operand = operand;
    this.curState = state;
    while (transitionList != null) {
      transitionList.appTransition.apply(this);
      transitionList = transitionList.next;
    }
    return new InternalStateMachine();
  }


  // =============== definition of TransitionList ==================
  private class TransitionList {
    ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> appTransition;
    TransitionList next;

    TransitionList(
        ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> appTransition,
        TransitionList next) {
      this.appTransition = appTransition;
      this.next = next;
    }
  }

  // ===============definition of ApplicableTransition ================
  private interface ApplicableTransition<
      OPERAND,
      STATE extends Enum<STATE>,
      EVENTTYPE extends Enum<EVENTTYPE>,
      EVENT> {
    void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> factory);
  }

  private class SingleArcApplicableTransition implements ApplicableTransition<
      OPERAND, STATE, EVENTTYPE, EVENT> {

    STATE oldState;
    STATE postState;
    EVENTTYPE type;
    SingleArcInternal transition;

    SingleArcApplicableTransition(
        STATE oldState,
        EVENTTYPE type,
        SingleArcInternal transition) {
      this.oldState = oldState;
      this.type = type;
      this.transition = transition;
    }

    @Override
    public void apply(
        StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> factory) {
      if (!factory.mp.containsKey(oldState)) {
        factory.mp.put(oldState, new HashMap<>());
      }
      factory.mp.get(oldState).put(type, transition);
    }
  }

  // =============== definition of transition =====================
  private interface Transition<
      OPERAND,
      STATE extends Enum<STATE>,
      EVENT> {
    STATE doTransition(OPERAND operand, EVENT event);
  }

  private class SingleArcInternal implements Transition<OPERAND, STATE, EVENT> {

    private STATE postState;
    private SingleArcTransition<OPERAND, STATE, EVENT> hook;

    SingleArcInternal(
        STATE postState,
        SingleArcTransition<OPERAND, STATE, EVENT> hook) {
      this.postState = postState;
      this.hook = hook;
    }

    @Override
    public STATE doTransition(OPERAND operand, EVENT event) {
      STATE state = hook.doTransition(operand, event);
      if (state != postState) {
        throw new RuntimeException(
            String.format("expected is %s but get %s", postState, state));
      }
      return postState;
    }
  }

  // ================ definition of internal state machine ==================
  private class InternalStateMachine
      implements StateMachine<STATE, EVENTTYPE, EVENT> {

    @Override
    public STATE doTransition(EVENTTYPE type, EVENT event) {
      return curState = mp.get(curState).get(type).doTransition(operand, event);
    }

    @Override
    public STATE getCurrentState() {
      return curState;
    }
  }
}
