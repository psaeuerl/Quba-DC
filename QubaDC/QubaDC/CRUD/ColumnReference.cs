using System;

namespace QubaDC.CRUD
{

    public class ColumnReference
    {
        /// <summary>
        /// References to a TableReference
        /// </summary>
        public String TableReference { get; set; }
        public String ColumnName { get; set; }

        internal bool IsTheSame(ColumnReference x)
        {
            if (x == null)
                return false;
            return this.TableReference == x.TableReference && this.ColumnName == x.ColumnName;
        }
    }
}