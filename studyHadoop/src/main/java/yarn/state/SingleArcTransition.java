package yarn.state;

/**
 * SingleArcTransition.
 */
interface SingleArcTransition<
    OPERAND,
    STATE extends Enum<STATE>,
    EVENT> {

  STATE doTransition(OPERAND operand, EVENT event);
}
