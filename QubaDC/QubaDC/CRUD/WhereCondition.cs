namespace QubaDC.CRUD
{

    public class WhereCondition
    {
        public ColumnReference LHO { get; set; }
        public ComparisionOperator Comparator { get; set; }
        public RightHandOperand RHO { get; set; }
    }
}