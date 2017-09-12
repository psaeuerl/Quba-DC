using QubaDC.CRUD;
using QubaDC.Restrictions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.SMO
{
    class IntegratedSMOHelper
    {
        public static Restriction GetBasiRestriction(String tableRef, String queryTimeVariable)
        {
            OperatorRestriction startTs = new OperatorRestriction()
            {
                LHS = new ColumnOperand()
                {
                    Column = new ColumnReference()
                    {
                        ColumnName = IntegratedConstants.StartTS,
                        TableReference = tableRef
                    }
                },
                Op = RestrictionOperator.LET
                ,
                RHS = new ValueRestrictionOperand()
                {
                    Value = queryTimeVariable
                }
            };
            OperatorRestriction endTsLt = new OperatorRestriction()
            {
                LHS = new ValueRestrictionOperand()
                {
                    Value = queryTimeVariable
                },
                Op = RestrictionOperator.LT
,
                RHS = new ColumnOperand()
                {
                    Column = new ColumnReference()
                    {
                        ColumnName = IntegratedConstants.EndTS,
                        TableReference = tableRef
                    }
                },
            };
            OperatorRestriction endTSNull = new OperatorRestriction()
            {
                LHS = new ColumnOperand()
                {
                    Column = new ColumnReference()
                    {
                        ColumnName = IntegratedConstants.EndTS,
                        TableReference = tableRef
                    }
                },
                Op = RestrictionOperator.IS
,
                RHS = new LiteralOperand()
                {
                    Literal = "NULL"
                }
            };
            var OrRestriction = new OrRestriction();
            OrRestriction.Restrictions = new Restriction[] { endTsLt, endTSNull };
            var AndRestriction = new AndRestriction();
            AndRestriction.Restrictions = new Restriction[] { startTs, OrRestriction };

            return AndRestriction;
        }
    }
}
