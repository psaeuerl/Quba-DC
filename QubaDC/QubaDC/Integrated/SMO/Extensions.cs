using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Integrated.SMO
{
    public static class Extensions
    {
        public static String AsScript(this IEnumerable<String> parts)
        {
            return String.Join(System.Environment.NewLine, parts.Select(x => x.Last() == ';' ? x : x + ";").ToArray());
        }
    }
}
