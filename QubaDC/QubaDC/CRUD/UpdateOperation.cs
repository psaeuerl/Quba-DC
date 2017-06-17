using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.CRUD
{
    public class UpdateOperation 
    {
        public Table Table { get; set; }

        public String[] ColumnNames { get; set; }
        public String[] ValueLiterals { get; set; }

        public Restriction Restriction { get; set; }
    }
}
