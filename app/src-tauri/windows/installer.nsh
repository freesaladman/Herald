; Adds $INSTDIR (where the herald CLI sidecar lives) to the per-user PATH.
; PowerShell does the edit because NSIS strings are capped at NSIS_MAX_STRLEN (1024),
; which would truncate a long PATH. It goes through the registry because the .NET Framework
; Environment API expands %VAR% entries and rewrites the value as REG_SZ. $INSTDIR is passed via HERALD_INSTDIR rather than
; interpolated into the command. In the scripts, $$ is a literal $.

Var HeraldRc

; Appends the install dir unless an entry already matches it, raw or expanded (trimmed, ignoring a trailing backslash, case-insensitive).
!define HERALD_PS_ADD "$$ErrorActionPreference='Stop'; try { $$d=$$env:HERALD_INSTDIR.Trim().TrimEnd('\'); $$k=[Microsoft.Win32.Registry]::CurrentUser.OpenSubKey('Environment',$$true); try { $$p=[string]$$k.GetValue('Path','',[Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames); $$kind=[Microsoft.Win32.RegistryValueKind]::ExpandString; try { $$kind=$$k.GetValueKind('Path') } catch {}; $$e=@(); if ($$p) { $$e=$$p -split ';' }; if (@($$e | Where-Object { $$x=$$_.Trim().TrimEnd('\'); ($$x -ieq $$d) -or ([Environment]::ExpandEnvironmentVariables($$x).TrimEnd('\') -ieq $$d) }).Count -gt 0) { exit 0 }; if ($$p) { $$n=$$p.TrimEnd(';') + ';' + $$env:HERALD_INSTDIR } else { $$n=$$env:HERALD_INSTDIR }; $$k.SetValue('Path',$$n,$$kind) } finally { $$k.Close() } } catch { exit 1 }"

; Filters out every matching entry, keeping the others untouched, and writes only if something was removed.
!define HERALD_PS_REMOVE "$$ErrorActionPreference='Stop'; try { $$d=$$env:HERALD_INSTDIR.Trim().TrimEnd('\'); $$k=[Microsoft.Win32.Registry]::CurrentUser.OpenSubKey('Environment',$$true); try { $$p=[string]$$k.GetValue('Path','',[Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames); if (-not $$p) { exit 0 }; $$kind=[Microsoft.Win32.RegistryValueKind]::ExpandString; try { $$kind=$$k.GetValueKind('Path') } catch {}; $$e=$$p -split ';'; $$m=@($$e | Where-Object { $$x=$$_.Trim().TrimEnd('\'); -not (($$x -ieq $$d) -or ([Environment]::ExpandEnvironmentVariables($$x).TrimEnd('\') -ieq $$d)) }); if ($$m.Count -ne $$e.Count) { $$k.SetValue('Path',($$m -join ';'),$$kind) } } finally { $$k.Close() } } catch { exit 1 }"

!macro NSIS_HOOK_POSTINSTALL
  System::Call 'Kernel32::SetEnvironmentVariable(t "HERALD_INSTDIR", t "$INSTDIR")i'
  nsExec::ExecToLog `"$SYSDIR\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -NonInteractive -ExecutionPolicy Bypass -Command "${HERALD_PS_ADD}"`
  Pop $HeraldRc
  SendMessage 0xFFFF 0x001A 0 "STR:Environment" /TIMEOUT=5000
  StrCmp $HeraldRc 0 +2
    DetailPrint "PATH not updated: add $INSTDIR manually"
!macroend

!macro NSIS_HOOK_POSTUNINSTALL
  System::Call 'Kernel32::SetEnvironmentVariable(t "HERALD_INSTDIR", t "$INSTDIR")i'
  nsExec::ExecToLog `"$SYSDIR\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -NonInteractive -ExecutionPolicy Bypass -Command "${HERALD_PS_REMOVE}"`
  Pop $HeraldRc
  SendMessage 0xFFFF 0x001A 0 "STR:Environment" /TIMEOUT=5000
  StrCmp $HeraldRc 0 +2
    DetailPrint "PATH not updated: remove $INSTDIR manually"
!macroend
