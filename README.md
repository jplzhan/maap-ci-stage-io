# maap-ci-stage-io
A bootstrapping repository for running the stage-in and stage-out steps for MAAP CI/CD build. This README also contains helpful information and tutorials on how to debug CWL files.

## How to Debug CWL Files
In order to debug the opaque features of CWL, our strategy should be to inject Javascript into the file. Take the following official CWL code as an example:

```cwl
cwlVersion: v1.0
class: CommandLineTool
baseCommand: cat

hints:
  DockerRequirement:
    dockerPull: alpine

inputs:
  in1:
    type: File
    inputBinding:
      position: 1
      # Asks CWL to submit this objects 'basename' field as an argument to the baseCommand
      valueFrom: $(self.basename)

requirements:
  InitialWorkDirRequirement:
    listing:
      # An array of files CWL will mount into the docker image, notice this references the in1 object itself
      - $(inputs.in1)

outputs:
  out1: stdout
```

### Problem
If you copy this example as-is and adapt it to call a script who expects a filename as an argument, there is a good chance that this will fail and you'll recieve a "file does not exist" error instead, even though CWL successfully detected and mounted the file.

Why does this happen? It is because `self.basename` is a Javascript expression accessing the `basename` field of the `self` object corresponding to `in1`. Meanwhile, `basename` refers to the filename passed in to `in1`. It **does not** include the directory within the docker container that the file is mounted to! This means that if your docker container uses a working directory which is different from the folder where the file `in1` is mounted to, then using only the filename is completely useless and your script will be incapable of finding it!

### Solution
To diagnose this issue using Javascript, we need to add the following line to the `requirements` dictionary within the CWL:

```cwl
requirements:
  InlineJavascriptRequirement: {}
```

This will allow CWL to run Javascript expressions using the `${}` expression (notice these are curly brackets, not parentheses).
Now we can replace `$(self.basename)` with the following:

```cwl
baseCommand: echo
inputs:
  in1:
    type: File
    inputBinding:
      position: 1
      # Defines a Javascript function whose return value will be used as the argument submitted to the baseCommand
      valueFrom: ${ return "\"" + JSON.stringify(self) + "\"; }
# other stuff...
```

By doing this, you will get to see the full object associated with `in1` as a string. As an example, we would see this string appear when the baseCommand gets run (prettified for the purposes of this README):

```json
{
  "class": "File",
  "location": "file:///data/home/hysdsops/zhan/artifact-deposit-repo/jplzhan/gedi-subset/main/geoBoundaries-GAB-ADM0.geojson",
  "size": 51350,
  "basename": "geoBoundaries-GAB-ADM0.geojson",
  "nameroot": "geoBoundaries-GAB-ADM0",
  "nameext": ".geojson",
  "path": "/PpwdnD/geoBoundaries-GAB-ADM0.geojson",
  "dirname": "/PpwdnD"
}
```

Notice here that there are many more fields than just `basename`. In short, we can guess that `self.dirname` refers to the directory within the docker image the CWL file is mounted into (and you can test that it is), and `self.path` is the absolute path to the mounted file within the docker image. In addition, you can use other parameters like `self.size` and `self.nameext` to perform operations relating to the size and type of file being used.

This information corresponds to `Fields` table described under `File` type inputs in the [official documenation](https://www.commonwl.org/v1.0/CommandLineTool.html#File), but using Javascript allows us to debug this information in an actual setting.
