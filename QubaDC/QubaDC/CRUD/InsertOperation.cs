﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.CRUD
{
    public class InsertOperation 
    {

        //INSERT INTO <TABLE> <COLUMNS> <VALUES>

        public Table InsertTable { get; set; }

        public String[] ColumnNames { get; set; }
        public String[] ValueLiterals { get; set; }


    }
}
