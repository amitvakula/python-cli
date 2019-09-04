# add import statements
import docker
import flywheel
from pick import pick
import pdb
from pprint import pprint
from build_flywheel_environment import create_container #, generate_gear_object, expand_flywhel_folder

def add_command(subparsers, parents):
    parser = subparsers.add_parser('create', parents=parents, help='Import a folder of dicom files')
    #parser.add_argument('', help='')


    parser.set_defaults(func=create_gear) # function name
    parser.set_defaults(parser=parser)

    return parser

def fetch_name(flywheel_client):
	return "{0} {1}".format(flywheel_client.firstname, flywheel_client.lastname)

def create_gear_prompts():
	print()

	print("Welcome! First, give your gear a friendly, human-readable name.")
	gearLabel = input("Human Readable Name: ") # runPrompt(gearLabelPrompt)
	print()

	print("Next, specify a gear ID. This is a unique, machine-friendly abbreviation.")
	gearName = input("Machine Friendly Name: ") # runPrompt(gearNamePrompt)
	print()

	print("Choose a Docker image to start your project.")
	title = 'Choose a Docker image to start your project:'
	options = ['Other', 'Python:3', 'Python:2', 'Ubuntu']
	baseImage, index = pick(options, title) # runSelectWA(gearImagePrompt)
	if baseImage == "Other":
		baseImage = input("Other: ")
	else: 
		print(baseImage)
	print()

	print("Is this a converter or an analysis gear?")
	title = 'Is this a converter or an analysis gear?'
	options = ['converter', 'analysis']
	gearType, index = pick(options, title) # runSelect(gearCategoryPrompt)
	print(gearType)
	print()

	return gearLabel, gearName, baseImage, gearType

def is_docker_image_local(docker_client, image_name):
	if (":" not in image_name) and ("@" not in image_name):
		print("Assuming 'latest' as the image tag.")
		image_name += ":latest"

	print("Checking if {0} is available locally...".format(image_name))
	images = docker_client.images.list()

	for image in images:
		for digest in image.attrs['RepoDigests']:
			if digest == image_name:
				print("\tFound digest locally.")
				return True

		for tag in image.tags:
			if tag == image_name:
				print("\tFound tag locally.")
				return True

	print("Image is not installed locally.")
	return False

def pull_docker_image(docker_client, image_name):
	print("Pulling {0} from registry...".format(image_name))
	docker_client.images.pull(image_name)
	print("\tImage downloaded.")

def ensure_docker_image_is_local(docker_client, image_name):
	if not is_docker_image_local(docker_client, image_name):
		pull_docker_image(docker_client, image_name)

# func (docker *D) InspectImage(imageName string) *types.ImageInspect {
# 	Println("Inspecting", imageName, "...")
# 	details, _, err := docker.ImageInspectWithRaw(ctx, imageName)
# 	Check(err)

# 	if details.Os != "linux" {
# 		FatalWithMessage("Unrecognized OS", details.Os, "must be linux")
# 	}

# 	if details.Architecture != "amd64" {
# 		FatalWithMessage("Unrecognized architecture", details.Architecture, "must be amd64")
# 	}

# 	Println("\tImage is the correct architecture.")

# 	return &details
# }

# Return details with info parsed into gear format
def get_docker_image_details(image_name):
	cli = docker.APIClient()
	details = cli.inspect_image(image_name)
	env = details['Config']['Env']

	if details['Os'] != "linux":
		raise RuntimeError("Unrecognized OS {0} must be linux".format(details['Os']))
	

	if details['Architecture'] != "amd64":
		raise RuntimeError("Unrecognized architecture {0} must be amd64".format(details['Architecture']))

	print("\tImage is the correct architecture.")

	return details, env

def create_gear(args):
	flywheel_client = user # get flywheel client
	docker_client = docker.from_env() # get docker client ... not sure if needed
	#d = docker.from_env() # create a Docker client

	# User info
	realName = fetch_name(flywheel_client)
	label, name, imageName, gType = create_gear_prompts()

	# Image
	ensure_docker_image_is_local(docker_client, imageName)

	details, env = get_docker_image_details(imageName)
	
	# Translate info to action
	#defaultManifest, modifier = generate_gear_object(label, name, imageName, gType, env, realName)


	# Inflate
	pdb.set_trace()
	cid, cleanup = create_container(docker_client, imageName) # docker_client.containers.create(imageName)
	#expand_flywhel_folder(docker_client, imageName, cid, defaultManifest, modifier)
	
	cleanup()

	print("\n")
	print("Your gear is created and expanded to the current directory.")
	print("Try `fw gear local` to run the gear!")

if __name__ == "__main__":
	fw = flywheel.Client('ucsfbeta.flywheel.io:3H4CWFtFk97JYIKqSE')
	user = fw.get_current_user()
	pprint(user)
	create_gear(None)