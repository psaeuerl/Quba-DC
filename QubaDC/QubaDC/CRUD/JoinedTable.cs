namespace QubaDC.CRUD
{

    public class JoinedTable : SelectTable
    {
        public JoinType Join { get; set; }
        public WhereCondition[] WhereConditions { get; set; }
    }
}