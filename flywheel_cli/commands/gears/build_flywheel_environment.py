
# Encapsulate the changes needed to modify an upstream manifest, or use a completely default one
# def generate_gear_object(label, name, imageName, category, env, realName):

# 	# basic := &api.Gear{
# 	# 	Name:        name,
# 	# 	Label:       label,
# 	# 	Description: "Created by the gear builder. Edit the manifest file to give this gear a description!",
# 	# 	Version:     "0",
# 	# 	Inputs: map[string]map[string]interface{}{
# 	# 		"dicom": {
# 	# 			"base": "file",
# 	# 			"type": map[string]interface{}{
# 	# 				"enum": []string{"dicom"},
# 	# 			},
# 	# 			"optional":    true,
# 	# 			"description": "Any dicom file.",
# 	# 		},
# 	# 		"api-key": {
# 	# 			"base": "api-key",
# 	# 		},
# 	# 	},
# 	# 	Config: map[string]map[string]interface{}{
# 	# 		"address": {
# 	# 			"default":     "Example",
# 	# 			"type":        "string",
# 	# 			"description": "String example: any text.",
# 	# 		},
# 	# 		"cost": {
# 	# 			"default":     3.5,
# 	# 			"type":        "number",
# 	# 			"description": "Float example: any real number.",
# 	# 		},
# 	# 		"age": {
# 	# 			"default":     7,
# 	# 			"type":        "integer",
# 	# 			"description": "Integer example: any whole number.",
# 	# 		},
# 	# 		"fast": {
# 	# 			"default":     false,
# 	# 			"type":        "boolean",
# 	# 			"description": "Boolean example: a toggle.",
# 	# 		},
# 	# 		"nickname": {
# 	# 			"default":     "Jimmy",
# 	# 			"minLength":   2,
# 	# 			"maxLength":   15,
# 	# 			"type":        "string",
# 	# 			"description": "String length example: 2 to 15 characters long.",
# 	# 		},
# 	# 		"phone": {
# 	# 			"default":     "555-5555",
# 	# 			"pattern":     "^[0-9]{3}-[0-9]{4}$",
# 	# 			"type":        "string",
# 	# 			"description": "String regex example: any phone number, no area code.",
# 	# 		},
# 	# 		"show-example": {
# 	# 			"default":     false,
# 	# 			"type":        "boolean",
# 	# 			"description": "Show example features in the gear script!",
# 	# 		},
# 	# 	},
# 	# 	Environment: map[string]string{
# 	# 		"Example_Environment_Variable": "Set gear environment variables here.",
# 	# 	},
# 	# 	Command:    "./example.sh --age {{age}} --cost {{cost}}",
# 	# 	Author:     realName,
# 	# 	Maintainer: realName,
# 	# 	Cite:       "List citations here.",
# 	# 	License:    "Other",
# 	# 	Source:     "",
# 	# 	Url:        "",

# 	# 	Custom: map[string]interface{}{
# 	# 		"gear-builder": &GearBuilderInfo{
# 	# 			Image:    imageName,
# 	# 			Category: category,
# 	# 		},
# 	# 	},
# 	# }

# 	# Use upstream environment decls if present
# 	if len(env) > 0:
# 		basic.Environment = env

# 	# Modifer function that incorporates GB changes into an upstream manifest.
# 	def modifier(g):
# 		g.Name = name
# 		g.Label = label
# 		g.Version = "0"
# 		g.Author = realName
# 		g.Maintainer = realName
# 		g.Custom = map[string]interface{}{}
# 		g.Custom["gear-builder"] = map[string]interface{}{
# 			"image":    imageName,
# 			"category": category,
# 		}

# 		# Merge environment map with upstream; upstream wins.
# 		if len(env) > 0:

# 			finalEnv = map[string]string{}

# 			for k, v in g.Environment:
# 				finalEnv[k] = v

# 			for k, v in env:
# 				finalEnv[k] = v

# 			# If a manifest key was overridden, verbosely report it.
# 			if len(finalEnv) < len(g.Environment)+len(env):
# 				print()
# 				print("Both the local manifest and the docker image have environment variables.")
# 				print()
# 				print("Original on disk:")
# 				print(g.Environment)
# 				print()
# 				print("Original from docker image:")
# 				print(env)
# 				print()
# 				print("The merged result:")
# 				print(finalEnv)
# 				print()
# 				print("If desired, you may edit the manifest file to edit this merge.")
# 				print()

# 				g.Environment = finalEnv

# 		return g

# 	return basic, modifier

# def expand_flywhel_folder(docker_client, imageName, containerId, defaultManifest, modifier):
# 	print("Checking if image has gear contents...")
# 	#reader, stat, err := docker.CopyFromContainer(ctx, containerId, GearPath)

# 	# If there is no gear path, or the path is not a directory, use example content
# 	if (err != nil && strings.Contains(err.Error(), "No such")) || (err == nil && !stat.Mode.IsDir()):
# 		print("\tNo gear contents. Providing a starter kit...")

# 		python = docker.PythonInstalled(imageName)

# 		# Write a nice script based on installed language
# 		if python:
# 			defaultManifest.Command = "./example.py"

# 			# Write example run script
# 			_, err = os.Stat("example.py")
# 			if err == nil:
# 				runConfirmFatal(confirmReplaceScriptPmtP)
# 			err = ioutil.WriteFile("example.py", []byte(ExamplePythonScript), 0750)
# 			Check(err)
# 		else:
# 			# Write example run script
# 			_, err = os.Stat("example.sh")
# 			if err == nil:
# 				runConfirmFatal(confirmReplaceScriptPmt)
# 			err = ioutil.WriteFile("example.sh", []byte(ExampleRunScript), 0750)
# 			Check(err)

# 		# Write manifest
# 		_, err = os.Stat(ManifestName)
# 		if err == nil:
# 			runConfirmFatal(confirmReplaceManifestPmt)
# 		err = ioutil.WriteFile(ManifestName, FormatBytes(defaultManifest), 0640)
# 		Check(err)

# 	else:
# 		Check(err)
# 		Println("\tExpanding gear contents...")
# 		err = UntarGearFolder(reader)

# 		# Can be nil
# 		localManifest = TryToLoadCWDManifest()

# 		if localManifest != nil:
# 			Println("Modifying local manifest...")
# 			localManifest = modifier(localManifest)

# 		else:
# 			# Should not happen to sane images since we checked for a gear folder already
# 			Println("Warning: no", ManifestName, "was present from the image despite having a ", GearPath, "folder. Generating a default one.")
# 			localManifest = defaultManifest

# 		err = ioutil.WriteFile(ManifestName, FormatBytes(localManifest), 0640)
# 		Check(err)

# 
def create_container(docker_client, imageName):
	return create_container_from_image(docker_client, imageName, None, None)

# 
def create_container_from_image(docker_client, imageName, config, hostConfig):
	print("Creating container from {0} ...".format(imageName))

	container = docker_client.containers.create(imageName)
	
	container_id = "fake_id"
	print("\tCreated {0}".format(container_id))

	def cleanup():
		print("Removing container {0} ...".format(container_id))
		# need to add code to remove a container
		print("\tRemoved container")

	return containerId, cleanup

# // Consistent way to launch a local gear.
# // Ensure image is local first
# func (docker *D) CreateContainerForGear(imageName string, env map[string]string, command []string, mounts []mount.Mount) (string, func()) {

# 	return docker.CreateContainerFromImage(imageName, &container.Config{
# 		WorkingDir: GearPath,
# 		Entrypoint: []string{}, // prevent upstream image from interfering
# 		Cmd:        command,
# 		Env:        TranslateEnvToEnvArray(env),
# 	},
# 		&container.HostConfig{
# 			Mounts: mounts,
# 		})
# }
