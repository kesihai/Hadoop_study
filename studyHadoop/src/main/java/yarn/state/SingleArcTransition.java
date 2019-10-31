package yarn.state;

interface SingleArcTransition<
    OPERAND,
    STATE extends Enum<STATE>,
    EVENT> {
  STATE transition(OPERAND operand, EVENT event);
}
