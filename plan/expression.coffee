# Is NULL a core data type?
# NUMBER null treated as 0?

datatypes: [
  Null
  Boolean
  Number
  NumberRange
  String
  Date
  TimeRange
  Set('*')

  Dataset

  Shape
  Scale
  Mark
]


Expression = [
  #NULL
  { op: 'literal', value: null }

  #BOOLEAN
  { op: 'literal', value: true/false }
  { op: 'ref', name: 'is_robot' }
  { op: 'is', lhs: T, rhs: T }
  { op: 'lessThan', lhs: NUMBER, rhs: NUMBER }
  { op: 'lessThanOrEqual', lhs: NUMBER, rhs: NUMBER }
  { op: 'greaterThan', lhs: NUMBER, rhs: NUMBER }
  { op: 'greaterThanOrEqual', lhs: NUMBER, rhs: NUMBER }
  { op: 'in', lhs: CATEGORICAL, rhs: STRING_SET }
  { op: 'in', lhs: NUMBER, rhs: NUMBER_RANGE }
  { op: 'in', lhs: TIME, rhs: TIME_RANGE }
  { op: 'match', regexp: '^\d+', operand: CATEGORICAL }
  { op: 'not', operand: BOOLEAN }
  { op: 'and', operands: [BOOLEAN, BOOLEAN, '...'] }
  { op: 'or', operands: [BOOLEAN, BOOLEAN, '...'] }

  #NUMBER
  { op: 'literal', value: 6 }
  { op: 'ref', name: 'revenue' }
  { op: 'add', operands: [NUMBER, NUMBER, '...'] }
  { op: 'negate', operand: NUMBER }
  { op: 'multiply', operands: [NUMBER, NUMBER, '...'] }
  { op: 'reciprocate', operand: NUMBER }
  { op: 'aggregate', operand: DATASET, fn: 'sum', attribute: EXPRESSION }

  #NUMBER_RANGE
  { op: 'literal', value: NumberRange() }
  { op: 'ref', name: 'revenue_range' }
  { op: 'numberBucket', operand: NUMBER, size: 0.05, offset: 0.01 }

  #TIME
  { op: 'literal', value: Date() }
  { op: 'ref', name: 'timestamp' }
  { op: 'timeOffset', operand: TIME, duration: 'P1D' }

  #TIME_RANGE
  { op: 'literal', value: TimeRange() }
  { op: 'ref', name: 'flight_time' }
  { op: 'timeBucket', operand: TIME, duration: 'P1D' }

  #STRING
  { op: 'literal', value: 'Honda' }
  { op: 'ref', type: 'categorical', name: 'make' }
  { op: 'concat', operands: [CATEGORICAL, CATEGORICAL, '...'] }

  #SET
  { op: 'literal', value: Set(['Honda', 'BMW', 'Suzuki']) }
  { op: 'ref', name: 'authors' }
  { op: 'aggregate', operand: DATASET, fn: 'group', attribute: EXPRESSION }

  #DATASET
  { op: 'literal', value: Dataset() }
  { op: 'label', operand: SET, name: 'blah' }
  { op: 'actions', operand: DATASET, actions: [Action, Action, '...'] }
]

Actions = [
  { action: 'apply', name: 'blah', expression: Expression }
  { action: 'filter', expression: Expression }
  { action: 'sort', expression: Expression, direction: 'ascending' }
  { action: 'limit', limit: 10 }
]

Expression = literal / ref / chain


{
  op: 'chain'
  operand: 5
  operands: [
    {
      op: 'add'
      expression: 10
    }
  ]
}

{
  op: 'chain'
  expression: $()
  actions: [
    {
      action: 'apply'
      name: 'data'
      expression: {
        op: 'chain'
        expression: '$data'
        actions: [
          {
            action: 'filter'
            expression: {
              op: 'chain'
              expression: '$color'
              actions: [
                { action: 'is', expression: 'D' }
                {
                  action: 'or'
                  expression: {
                    op: 'chain'
                    expression: '$color'
                    action: { action: 'is', expression: 'E' }
                  }
                }
                {
                  action: 'or'
                  expression: {
                    op: 'chain'
                    expression: '$color'
                    action: { action: 'is', expression: 'F' }
                  }
                }
              ]
            }
          }
        ]
      }
    }
    {
      action: 'apply'
      name: 'Count'
      expression: {
        op: 'chain'
        expression: '$data'
        actions: [
          { action: 'count' }
        ]
      }
    }
    {
      action: 'apply'
      name: 'Pages'
      expression: {
        op: 'chain'
        expression: '$data'
        actions: [
          {
            action: 'split'
            expression: '$page'
            dataName: 'data'
          }
          {
            action: 'apply'
            name: 'Count'
            expression: {
              op: 'chain'
              expression: '$data'
              actions: [
                { action: 'count' }
                { action: 'multiply', expression: 2 }
              ]
            }
          }
          {
            action: 'sort'
            expression: '$Count'
            direction: 'descending'
          }
          {
            action: 'limit'
            limit: 5
          }
        ]
      }
    }
  ]
}


