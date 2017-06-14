using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.CRUD
{
    public class DeleteOperation 
    {
        public Table Table { get; set; }

        public Restriction Restriction { get; set; } = null;
    }
}
