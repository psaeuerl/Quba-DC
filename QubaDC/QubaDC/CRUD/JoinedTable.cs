﻿using System;

namespace QubaDC.CRUD
{

    public class JoinedTable : SelectTable
    {
        public JoinType Join { get; set; }
        public Restriction JoinCondition { get; set; }


    }
}