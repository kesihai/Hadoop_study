package yarn.state;

/**
 * StageMachine Interface.
 */
interface StateMachine<
    STATE extends Enum<STATE>,
    EVENTTYPE extends Enum<EVENTTYPE>,
    EVENT> {

  STATE getCurrentState();

  STATE doTransition(EVENTTYPE type, EVENT event);
}
