﻿using QubaDC.Restrictions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC
{
    public abstract class RestrictionOperand
    {
        public abstract T Accept<T>(RestrictionTreeTraverser<T> visitor);

    }
}
