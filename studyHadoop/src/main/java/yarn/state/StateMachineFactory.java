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

  private TransitionList<OPERAND, STATE, EVENTTYPE, EVENT> transitionList;
  private Map<STATE,
      Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>> mp;

  public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(
      STATE curState,
      EVENTTYPE type,
      SingleArcTransition<OPERAND, STATE, EVENT> transition,
      STATE targetState) {
    this.transitionList = new TransitionList<>(
        new SingleOrMultipleApplicableTransition(
            curState, type, new SingleInternalArc(targetState, transition)),
        this.transitionList);
    return this;
  }

  private STATE doTransition(
      STATE state, OPERAND operand, EVENTTYPE type, EVENT event) {
    System.out.println(state + " " + type);
    return mp.get(state).get(type).doTransition(operand, event);
  }

  private class InternalStateMachine
      implements StateMachine<STATE, EVENTTYPE, EVENT> {
    private STATE curState;
    final private OPERAND operand;

    InternalStateMachine(STATE curState, OPERAND operand) {
      this.curState = curState;
      this.operand = operand;
    }

    @Override
    public STATE doTransition(EVENTTYPE type, EVENT event) {
      return curState =
          StateMachineFactory.this.doTransition(curState, operand, type, event);
    }

    @Override
    public STATE getCurrentState() {
      return curState;
    }
  }

  StateMachine<STATE, EVENTTYPE, EVENT> make(
      STATE state, final OPERAND operand) {
    mp = new HashMap<>();
    while (transitionList != null) {
      transitionList.transition.apply(this);
      transitionList = transitionList.next;
    }
    return new InternalStateMachine(state, operand);
  }

  private class TransitionList<
      OPERAND,
      STATE extends Enum<STATE>,
      EVENTTYPE extends Enum<EVENTTYPE>,
      EVENT> {
    ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition;
    TransitionList<OPERAND, STATE, EVENTTYPE, EVENT> next;

    TransitionList(
        ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition,
        TransitionList next) {
      this.transition = transition;
      this.next = next;
    }
  }

  private interface ApplicableTransition<
      OPERAND,
      STATE extends Enum<STATE>,
      EVENTTYPE extends Enum<EVENTTYPE>,
      EVENT> {
    void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> m);
  }

  private class SingleOrMultipleApplicableTransition
      implements ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> {

    private STATE state;
    private EVENTTYPE type;
    private Transition<OPERAND, STATE, EVENTTYPE, EVENT> hook;

    SingleOrMultipleApplicableTransition(
        STATE state,
        EVENTTYPE type,
        Transition<OPERAND, STATE, EVENTTYPE, EVENT> hook) {
      this.state = state;
      this.type = type;
      this.hook = hook;
    }

    @Override
    public void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> m) {
      if (!m.mp.containsKey(state)) {
        mp.put(state, new HashMap<>());
      }
      mp.get(state).put(type, hook);
    }
  }

  private interface Transition<
      OPERAND,
      STATE extends Enum<STATE>,
      EVENTTYPE extends Enum<EVENTTYPE>,
      EVENT> {
    STATE doTransition(OPERAND operand, EVENT event);
  }

  private class SingleInternalArc implements Transition<
      OPERAND, STATE, EVENTTYPE, EVENT> {

    private SingleArcTransition<OPERAND, STATE, EVENT> hook;
    private STATE postState;

    SingleInternalArc(
        STATE postState,
        SingleArcTransition<OPERAND, STATE, EVENT> hook) {
      this.postState = postState;
      this.hook = hook;
    }

    @Override
    public STATE doTransition(OPERAND operand, EVENT event) {
      STATE curState = hook.transition(operand, event);
      if (curState != postState) {
        throw new RuntimeException("Invalid state exception");
      }
      return curState;
    }
  }

}
