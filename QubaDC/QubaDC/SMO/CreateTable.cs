﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.SMO
{
    public class CreateTable : SchemaModificationOperator
    {
        public override void Accept(SMOVisitor visitor)
        {
            visitor.Visit(this);
        }
    }
}
