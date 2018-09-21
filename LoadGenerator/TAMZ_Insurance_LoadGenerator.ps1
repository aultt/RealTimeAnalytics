$PortfolioFilesToGenMin = 10
$PorfolioFilesTOGenMax =50
$ComplaintsToGenMin =5
$ComplaintsToGenMax =30
$PolicesToGenMin = 1
$PolicesToGenMax =43
$MongoToGenMin =10
$MongoToGenMax = 50


$TimetoRun = 5 #minutes
$Loopinterval = 5 #seconds
$BatchInterval = 15 #millisec

$foldertoCheck= "\\ohnas001\WorkFolder\StreamFiles"
$ArchiveFolder = "\\ohnas001\WorkFolder\ArchivedFiles"
$FilestoMoveFolder = "\\ohnas001\WorkFolder\FilestoMove"
$ScratchlocationPortfolioFiles = "e:\scratch"
$securitiesFileLocation = "e:\command\resourcefiles\securities.txt"
$StorageUrl = "<Your Storage URL>"
$key = "<Your StorageKey>"
$SqlServer = "<Your SQL Server Name>"
$SQLDatabase = "TAMZ_Insurance"
$PortfolioFilesToGen =Get-Random -Minimum $PortfolioFilesToGenMin -Maximum $PorfolioFilesTOGenMax

$timeout = new-timespan -Minutes $TimetoRun
$sw = [diagnostics.stopwatch]::StartNew()
while ($sw.elapsed -lt $timeout){
    #Generate Random Numbers to Generate
    $PortfolioFilesToGen =Get-Random -Minimum $PortfolioFilesToGenMin -Maximum $PorfolioFilesTOGenMax
    $ComplaintsToGen = Get-Random -Minimum $ComplaintsToGenMin -Maximum $ComplaintsToGenMax
    $ComplaintsToUpdate = Get-Random -Minimum $ComplaintsToGenMin -Maximum $ComplaintsToGenMax
    $PoliciesToGen =Get-Random -Minimum $PolicesToGenMin -Maximum $PolicesToGenMax
    $MongoToGen = Get-Random -Minimum $MongoToGenMin -Maximum $MongoToGenMax

    #Generate Portfolio Files 
    E:\command\HPCModels\hpcmodels.exe /MY /O$ScratchlocationPortfolioFiles /S$securitiesFileLocation /L$PortfolioFilesToGen /V1 /N5 /X10
    $moveFromPath = Join-Path -Path $ScratchlocationPortfolioFiles -ChildPath "*.*"
    Move-Item -Path $moveFromPath -Destination $foldertoCheck
    
    
    $files = Get-ChildItem $foldertoCheck
    foreach ($File in $files)
    {
            $SourcePath = Join-Path -Path $foldertoCheck -ChildPath $File.name
            $DestinationFilePath = Join-Path -Path $FilestoMoveFolder -ChildPath $($file.Name)
            Move-Item -Path $SourcePath -Destination $DestinationFilePath 
    }

    AZCopy /Source:$FilesToMoveFolder /Dest:$StorageUrl /DestKey:$key /XO
    
    $ArchiveFiles = Get-ChildItem $FilestoMoveFolder
    foreach ($ArchiveFile in $ArchiveFiles)
    {
            $ArchivedFilesPath = Join-Path -Path $FilestoMoveFolder  -ChildPath $($ArchiveFile.Name)
            Move-Item -Path $ArchivedFilesPath -Destination $ArchiveFolder
    }

    #Generate New Complaints
    For ($i=0; $i -le $ComplaintsToGen; $i++) {
        $MinPolicy = invoke-sqlcmd -ServerInstance $SqlServer -Database $SQLDatabase -Query 'select Min(PolicyID)as ID from dbo.Policy'
        $MaxPolicy = invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query 'select Max(PolicyID)as ID from dbo.Policy'

        $ComplaintMsgtoGet =Get-Random -Minimum 1 -Maximum 30
        $ComplaintMsgQry = "SELECT Response FROM [FakeNameStore].[dbo].[ComplaintMessage] where ID = $ComplaintMsgtoGet"
        $ComplaintMsg = $(Invoke-Sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query $ComplaintMsgQry).Response
        $PolicyToUpdate = Get-Random -Minimum $MinPolicy.ID -Maximum $MaxPolicy.ID
        $Details = Get-Random -InputObject "Colission", "Comprehensive"
        $ClaimInsertQuery = "INSERT INTO dbo.Claim (PolicyId,Details,ClaimOpenDate,ClaimCloseDate,ClaimStatus) VALUES ($PolicyToUpdate,'$Details',GetDate(),null,'Pending')"
        $out=invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query $ClaimInsertQuery
        Start-Sleep -Milliseconds $BatchInterval

        $MinPolicyHolder = invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query 'select Min(PolicyHolderID)as ID from dbo.PolicyHolder'
        $MaxPolicyHolder = invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query 'select Max(PolicyHolderID)as ID from dbo.PolicyHolder'

        $PolicyHolderToUpdate = Get-Random -Minimum $MinPolicyHolder.ID -Maximum $MaxPolicyHolder.ID
        $Sentiment = Get-Random -InputObject "Angry","Happy","Sad","Content"

        $CompaintInsertQuery = "INSERT INTO dbo.Complaint (PolicyHolderId,ComplaintStatus,Sentiment,ComplaintOpenDate,ComplaintCloseDate,ComplaintMessage) VALUES ($PolicyHolderToUpdate,'Pending','$Sentiment',GetDate(),null,'$ComplaintMsg')"
        invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query $CompaintInsertQuery
    }
    write-host  "$ComplaintstoGen New Complaints Added"

    #Complaints to Update
    
    For ($i=0; $i -le $ComplaintsToUpdate; $i++) {
        $MinClaim = invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query 'select Min(ClaimID)as ID from dbo.Claim where ClaimCloseDate is null'
        $MaxClaim = invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query 'select Max(ClaimID)as ID from dbo.Claim where ClaimCloseDate is null'
        
        $ComplaintMsgtoGet =Get-Random -Minimum 1 -Maximum 30
        $ComplaintMsgQry = "SELECT Response FROM [FakeNameStore].[dbo].[ComplaintMessage] where ID = $ComplaintMsgtoGet"
        $ComplaintMsg = $(Invoke-Sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query $ComplaintMsgQry).Response
    
        $ClaimToUpdate = Get-Random -Minimum $MinClaim.ID -Maximum $MaxClaim.ID
        $ClaimStatus = Get-Random -InputObject "Open", "Unresolved","Closed"
        if (($ClaimStatus -eq "Unresolved") -or ($ClaimStatus -eq "Closed"))
        {$ClaimCloseDate = Get-date} 
        else {$ClaimCloseDate = $null}
    
        $ClaimsQuery = "UPDATE dbo.Claim SET ClaimCloseDate = '$ClaimCloseDate', ClaimStatus = '$ClaimStatus' WHERE ClaimID = $ClaimToUpdate"
        $out=invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query $ClaimsQuery
        Start-Sleep -Milliseconds $BatchInterval
    
        $MinComplaint = invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query 'select Min(ComplaintsID)as ID from dbo.Complaint where ComplaintCloseDate is null'
        $MaxComplaint = invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query 'select Max(ComplaintsID)as ID from dbo.Complaint where ComplaintCloseDate is null'
    
        $ComplaintToUpdate = Get-Random -Minimum $MinComplaint.ID -Maximum $MaxComplaint.ID
        $Sentiment = Get-Random -InputObject "Angry","Happy","Sad","Content"
        $ComplaintStatus = Get-Random -InputObject "Open", "Unresolved","Closed"
        if (($ComplaintStatus -eq "Unresolved") -or ($ComplaintStatus -eq "Closed"))
        {$ComplaintCloseDate = Get-date} 
        else {$ComplaintCloseDate = $null}
    
        if ($ComplaintStatus -eq "Open")
        {$CompaintQuery = "UPDATE dbo.Complaint SET ComplaintStatus = '$complaintStatus', Sentiment='$Sentiment',ComplaintMessage = '$ComplaintMsg' WHERE ComplaintsID = $ComplaintToUpdate "}
        else
        {$CompaintQuery = "UPDATE dbo.Complaint SET ComplaintStatus = '$complaintStatus', Sentiment='$Sentiment',ComplaintCloseDate = '$ComplaintCloseDate',ComplaintMessage = '$ComplaintMsg' WHERE ComplaintsID = $ComplaintToUpdate "}
        invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query $CompaintQuery
    }
    write-host  "$ComplaintsToUpdate Complaints Updated"
    
    #Generate New Policies
    $RanddomPolicyHolder = Get-Random -Minimum 7 -Maximum 2906
    $PolicyHolders = invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query "select TOP $PoliciesToGen PolicyHolderID from dbo.PolicyHolder WHERE PolicyHolderID > $RanddomPolicyHolder "
    
    foreach ($PolicyHolder in $PolicyHolders.PolicyHolderID)
    {
        
        $PolicyType = Get-Random -InputObject "Umbrella", "Auto", "Home","Boat","Motorcycle","Aircraft","Farm"
        SWITCH ($PolicyType)
        {
            Umbrella{
                $PolicyValue = Get-Random -Minimum 500000 -Maximum 2000000
                $PolicyCost = Get-Random -Minimum 75 -maximum 225     
            }
            Auto{
                $PolicyValue = Get-Random -Minimum 8000 -Maximum 100000
                $PolicyCost = Get-Random -Minimum 300 -maximum 2000 
            }
            Home{
                $PolicyValue = Get-Random -Minimum 100000 -Maximum 2000000
                $PolicyCost = Get-Random -Minimum 500 -maximum 2000 
            }
            Motorcycle{
                $PolicyValue = Get-Random -Minimum 5000 -Maximum 25000
                $PolicyCost = Get-Random -Minimum 75 -maximum 200 
            }
            Aircraft{
                $PolicyValue = Get-Random -Minimum 50000 -Maximum 250000
                $PolicyCost = Get-Random -Minimum 300 -maximum 1200 
            }
            Farm{
                $PolicyValue = Get-Random -Minimum 500000 -Maximum 2000000
                $PolicyCost = Get-Random -Minimum 300 -maximum 2500
            }
        }
        $Query = "insert into dbo.Policy(PolicyHolderID,PolicyType,PolicyValue,PolicyCost) Values ($PolicyHolder,'$PolicyType',$PolicyValue,$PolicyCost)"
        invoke-sqlcmd -ServerInstance $SqlServer -Database $SqlDatabase -Query $Query
        Start-Sleep -Milliseconds $BatchInterval
    }
    write-host  "$PoliciesToGen Policies Added"
    
    #Generate Mongo Amort
    Write-Host "Creating $MongoToGen Loans"
    E:\command\HPCModels\hpcmodels.exe /MG /Oc:\temp\in /v1000 /L$MongoToGen /N50000 /X1200000
    Start-Sleep -Milliseconds $BatchInterval
    E:\command\HPCModels\hpcmodels.exe /MA /Oc:\temp\out /Ic:\temp\in /N-1 /X1 /Re:\command\HPCModels\rates.txt /Te:\command\HPCModels\terms.txt
    Start-Sleep -Milliseconds $BatchInterval
    E:\Command\amortloader\amortloader.exe /minsert /c$MongoToGen /ic:\temp\out /w1 /smongodb://ohmgo001
    Remove-Item -Path c:\temp\in\*.*
    
    write-host  "Sleeping for $Loopinterval seconds"
    start-sleep -seconds $Loopinterval
}
 
write-host "Execution Loop Complete: Ran for $TimetoRun Min"