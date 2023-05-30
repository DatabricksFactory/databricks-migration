# Key vault name
$VaultName = '{fill with key vault name}' # Key vault name
# Users' principal names array
$UsersPN = @("{fill with your user id}") # User email id
# Users' keys access policies array
$KeysAP = @("all")
# Users' secrets access policies array
$SecretsAP = @("set,get,list,delete")
 
For ($i = 0; $i -lt $UsersPN.Length; $i++) {
    Write-Host "*** Granting access policies to [$($UsersPN[$i])]"
    Set-AzKeyVaultAccessPolicy -VaultName $VaultName -UserPrincipalName $UsersPN[$i] `
        -PermissionsToKeys ($KeysAP[$i]).Split(',') -PermissionsToSecrets ($SecretsAP[$i]).Split(',')
}
Write-Host "*** Completed"
