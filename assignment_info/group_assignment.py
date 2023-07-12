import json

info = {
    "groupName": "group_7",
    "sshGitHub": "git@github.com:ameirem/EfDS_group_7.git",
    "student_numbers": [
        "s3781976", "s3784657", "s3780767", "s3451887", "s3491455", "s3821749"
    ]
}

with open( "group_assignment.json", "w" ) as f:
    json.dump( obj = info, fp = f )
