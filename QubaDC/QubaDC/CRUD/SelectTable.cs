﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.CRUD
{
    public abstract class SelectTable : Table
    {
        public String TableAlias { get; set; }
    }
}
