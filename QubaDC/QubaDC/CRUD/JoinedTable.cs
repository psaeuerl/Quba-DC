namespace QubaDC.CRUD
{

    public class JoinedTable
    {
        public JoinType Join { get; set; }
        public WhereCondition[] WhereConditions { get; set; }
    }
}