[Setup]
AppId={{C2D39E02-13EB-43B4-9774-96CE84629B14}
AppName=AirVote
AppVersion=2.0.0
DefaultDirName={localappdata}\Programs\VotingStudio
DefaultGroupName=AirVote
PrivilegesRequired=lowest
OutputDir=..\dist\installer
OutputBaseFilename=VotingStudio-2.0.0-Setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
[Files]
Source: "..\dist\VotingStudio\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
[Icons]
Name: "{group}\AirVote"; Filename: "{app}\VotingStudio.exe"
Name: "{group}\Configure station server"; Filename: "{app}\VotingStudio.exe"; Parameters: "--configure"
Name: "{autodesktop}\AirVote"; Filename: "{app}\VotingStudio.exe"
[Run]
Filename: "{app}\VotingStudio.exe"; Description: "Open AirVote"; Flags: nowait postinstall skipifsilent
