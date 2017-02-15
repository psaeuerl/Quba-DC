using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.DatabaseObjects
{
    public class Table
    {
        public String Name { get; set; }
        public String Schema { get; set; }

        public String[] Columns { get; set; }

        public Table()
        {
            this.Name = String.Empty;
            this.Schema = String.Empty; ;
            this.Columns = new string[] { };
        }

        public Table(String Schema, String name, params String[] columns)
        {
            this.Name = name;
            this.Schema = Schema;
            this.Columns = columns;
        }
    }
}
