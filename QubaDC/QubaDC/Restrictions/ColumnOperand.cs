namespace QubaDC.CRUD
{

    public class ColumnOperand : RestrictionOperand
    {
        public ColumnReference Column { get; set; }

        public override T Accept<T>(RestrictionTreeTraverser<T> visitor)
        {
            return visitor.Visit(this);
        }

    }
}