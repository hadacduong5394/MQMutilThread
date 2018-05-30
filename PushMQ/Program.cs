using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace PushMQ
{
    internal class Program
    {
        private const int MF_BYCOMMAND = 0x00000000;
        public const int SC_CLOSE = 0xF060;

        [DllImport("user32.dll")]
        public static extern int DeleteMenu(IntPtr hMenu, int nPosition, int wFlags);

        [DllImport("user32.dll")]
        private static extern IntPtr GetSystemMenu(IntPtr hWnd, bool bRevert);

        [DllImport("kernel32.dll", ExactSpelling = true)]
        private static extern IntPtr GetConsoleWindow();

        private static void Main(string[] args)
        {
            var proc = new Process();
            proc.Start();
            Console.CancelKeyPress += delegate
            {
                proc.Stop();
                while (proc.IsRunning)
                    Thread.Sleep(1000);
            };
            Console.ReadLine();
        }
    }
}