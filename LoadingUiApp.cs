using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;

internal static class LoadingUiApp
{
    private const string LauncherName = "run-with-ui.ps1";
    private const string DemoName = "demo-interactive-ui.sh";
    private const string LauncherResource = "LoadingUI.Resources.run-with-ui.ps1";
    private const string DemoResource = "LoadingUI.Resources.demo-interactive-ui.sh";

    [STAThread]
    private static int Main(string[] args)
    {
        Console.Title = "Interactive Loading UI";
        Console.OutputEncoding = new UTF8Encoding(false);

        string temporaryDirectory = null;
        int exitCode = 1;
        try
        {
            temporaryDirectory = Path.Combine(
                Path.GetTempPath(),
                "LoadingUI-" + Guid.NewGuid().ToString("N")
            );
            Directory.CreateDirectory(temporaryDirectory);

            string launcherPath = Path.Combine(temporaryDirectory, LauncherName);
            ExtractResource(LauncherResource, launcherPath);

            string scriptPath;
            if (args.Length > 0)
            {
                scriptPath = Path.GetFullPath(args[0]);
                if (!File.Exists(scriptPath))
                {
                    throw new FileNotFoundException("Bash script not found.", scriptPath);
                }
            }
            else
            {
                scriptPath = Path.Combine(temporaryDirectory, DemoName);
                ExtractResource(DemoResource, scriptPath);
            }

            string bashPath = FindBash();
            if (bashPath == null)
            {
                throw new FileNotFoundException(
                    "Bash was not found. Install Git Bash or set the BASH_EXE environment variable."
                );
            }

            string powershellPath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.System),
                "WindowsPowerShell",
                "v1.0",
                "powershell.exe"
            );
            if (!File.Exists(powershellPath))
            {
                powershellPath = "powershell.exe";
            }

            ProcessStartInfo startInfo = new ProcessStartInfo();
            startInfo.FileName = powershellPath;
            startInfo.Arguments =
                "-NoLogo -NoProfile -ExecutionPolicy Bypass -File " + Quote(launcherPath) +
                " -ScriptPath " + Quote(scriptPath) +
                " -BashPath " + Quote(bashPath);
            startInfo.WorkingDirectory = Path.GetDirectoryName(scriptPath);
            startInfo.UseShellExecute = false;

            using (Process process = Process.Start(startInfo))
            {
                process.WaitForExit();
                exitCode = process.ExitCode;
            }
        }
        catch (Exception exception)
        {
            TryDeleteDirectory(temporaryDirectory);
            return ShowError("Unable to start the bundled loading UI: " + exception.Message);
        }

        TryDeleteDirectory(temporaryDirectory);

        Console.WriteLine();
        if (exitCode == 0)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Application completed successfully.");
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Application exited with code " + exitCode + ".");
        }
        Console.ResetColor();
        PauseBeforeClose();
        return exitCode;
    }

    private static void ExtractResource(string resourceName, string destinationPath)
    {
        Assembly assembly = Assembly.GetExecutingAssembly();
        using (Stream input = assembly.GetManifestResourceStream(resourceName))
        {
            if (input == null)
            {
                throw new InvalidOperationException("Embedded resource is missing: " + resourceName);
            }

            using (FileStream output = File.Create(destinationPath))
            {
                input.CopyTo(output);
            }
        }
    }

    private static void TryDeleteDirectory(string path)
    {
        if (String.IsNullOrWhiteSpace(path))
        {
            return;
        }

        try
        {
            string fullPath = Path.GetFullPath(path);
            string tempRoot = Path.GetFullPath(Path.GetTempPath());
            if (fullPath.StartsWith(tempRoot, StringComparison.OrdinalIgnoreCase) && Directory.Exists(fullPath))
            {
                Directory.Delete(fullPath, true);
            }
        }
        catch
        {
            // Temporary cleanup should not hide the application result.
        }
    }

    private static string FindBash()
    {
        string configured = Environment.GetEnvironmentVariable("BASH_EXE");
        if (!String.IsNullOrWhiteSpace(configured) && File.Exists(configured))
        {
            return configured;
        }

        string[] commonLocations = new string[]
        {
            Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles),
                "Git",
                "bin",
                "bash.exe"
            ),
            Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles),
                "Git",
                "usr",
                "bin",
                "bash.exe"
            )
        };

        foreach (string candidate in commonLocations)
        {
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        string path = Environment.GetEnvironmentVariable("PATH") ?? String.Empty;
        foreach (string directory in path.Split(Path.PathSeparator))
        {
            if (String.IsNullOrWhiteSpace(directory))
            {
                continue;
            }

            try
            {
                string candidate = Path.Combine(directory.Trim(), "bash.exe");
                if (File.Exists(candidate))
                {
                    return candidate;
                }
            }
            catch
            {
                // Ignore malformed PATH entries and continue searching.
            }
        }

        return null;
    }

    private static string Quote(string value)
    {
        return "\"" + value.Replace("\"", "\\\"") + "\"";
    }

    private static int ShowError(string message)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine("Loading UI could not start");
        Console.ResetColor();
        Console.WriteLine(message);
        PauseBeforeClose();
        return 1;
    }

    private static void PauseBeforeClose()
    {
        Console.WriteLine();
        Console.ForegroundColor = ConsoleColor.DarkGray;
        Console.Write("Press any key to close...");
        Console.ResetColor();
        Console.ReadKey(true);
    }
}
