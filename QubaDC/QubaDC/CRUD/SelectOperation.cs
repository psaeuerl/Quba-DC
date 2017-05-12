using System;
using System.Linq;
using QubaDC.SMO;
using System.Collections.Generic;

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
        public FromTable FromTable { get; set; }

        public JoinedTable[] JoinedTables { get; set; } = new JoinedTable[] { };


        public override void Accept(CRUDVisitor visitor)
        {
            visitor.Visit(this);
        }

        public static SelectOperation FromCreateTable(CreateTable t)
        {
            String reference = t.TableName + "_ref";
            return new SelectOperation()
            {
                Columns = t.Columns.Select(x => new ColumnReference()
                {
                    ColumnName = x.ColumName,
                    TableReference = reference
                }).ToArray()
                 ,
                FromTable = t.ToFromTable(reference)
            };
        }

        internal IEnumerable<SelectTable> GetAllSelectedTables()
        {
            yield return this.FromTable;
            foreach (var x in this.JoinedTables)
                yield return x;
        }
    }
}
