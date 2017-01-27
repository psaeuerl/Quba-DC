using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QubaDC.Utility
{
    public class Guard
    {
        public static void ArgumentNotNullOrWhiteSpace(String value,String name)
        {
            ThrowArgumentException<String>(String.IsNullOrWhiteSpace, value, name, "Argument: {0} was null or whitespace");
        }

        private static void ThrowArgumentException<T>(Func<T,Boolean> pre,T value, String Name, String Messge)
        {
            if(pre(value))
            {
                String msg = String.Format(Messge, Name, value);
                throw new ArgumentException(msg);
            }
        }
    }
}
