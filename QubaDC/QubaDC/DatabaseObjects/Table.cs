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
    }
}
