namespace QubaDC.CRUD
{

    //Example of an Select Statement:
    //Select a.1, a.2, b.b1, b.b2
    //FROM  ATable a
    //INNER JOIN TABLE BTable b on a.1 = b.b1
    //WHERE a.1 = 10
    //What do we not cover here?
    //Group By
    //Having
    //Aggregations
    //Functions
    //Additionally ... WhereConditions are AND concatenated
    //Can be easily extended for it
    //
    public class SelectOperation : CRUDOperation
    {

        public ColumnReference[] Columns { get; set; }
        public SelectTable FromTable { get; set; }

        public JoinedTable[] JoinedTable { get; set; }

        //TODO
        public override void Accept(CRUDVisitor visitor)
        {
            visitor.Visit(this);
        }
    }
}
