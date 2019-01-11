from common import const
import os


def coalesce_folder(output_file_path_with_prefix, append=False):

    for folder in os.listdir(output_file_path_with_prefix):

        if folder.startswith("_1"):

            folder_split = folder.split("=")

            if len(folder_split) != 2:
                raise ValueError("Folder label is not in proper format!")

            key = folder_split[1]

            if append:
                append_string = ">>"
            else:
                const.delete_file(output_file_path_with_prefix + str(key) + ".txt")
                append_string = ">"
            command = "cat " + output_file_path_with_prefix + "/" + folder + \
                      "/p* " + append_string + output_file_path_with_prefix + str(key) + ".txt"

            os.system(command)
            const.delete_file(output_file_path_with_prefix + "/" + folder)

    const.delete_file(output_file_path_with_prefix)


def coalesce_file(input_file_path, output_file_path, append=False):

    if append:
        append_string = ">>"
    else:
        append_string = ">"

    os.system("cat " + input_file_path + "/p* " + append_string + output_file_path)
    const.delete_file(input_file_path)


if __name__ == '__main__':
    pass
