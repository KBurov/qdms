// Imported from https://sentineliqf.codeplex.com

namespace QDMSServer.DataSources
{
    public static class Util
    {
        public static string ArrayToString(string[] arr, char delimeter)
        {
            string ret = "";
            if (arr == null) {
                return ret;
            }
            for (int i = 0; i < arr.Length; i++) {
                if (i > 0) {
                    ret += delimeter;
                }
                ret += arr[i];
            }
            return ret;
        }
    }
}