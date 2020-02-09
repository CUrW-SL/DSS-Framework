# Copy template and exe directories to daily hour folder and execute exe.
import os
from distutils.dir_util import copy_tree

COMPLETION_FILE = 'HYCHAN.OUT'


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def execute_flo2d(dir_path, run_date, run_time):
    print("Flo2d run_date : ", run_date)
    print("Flo2d run_time : ", run_time)
    output_dir = dir_path
    template_dir = os.path.join(os.getcwd(), 'template')
    # executable_dir = os.path.join(os.getcwd(), 'RunForProjectFolder')
    try:
        copy_tree(template_dir, output_dir)
        # copy_tree(executable_dir, output_dir)
        try:
            os.chdir(output_dir)
            try:
                os.system(os.path.join(output_dir, 'FLOPRO.exe'))
            except Exception as exx:
                print('Execute FLOPRO.exe|Exception : ', str(exx))
        except Exception as ex:
            print('Change the current working directory|Exception : ', str(ex))
    except Exception as e:
        print('template/executable copy error|Exception : ', str(e))


def flo2d_model_completed(dir_path, run_date, run_time):
    print("Flo2d run_date : ", run_date)
    print("Flo2d run_time : ", run_time)
    output_dir = dir_path
    completed = False
    try:
        result_file_name = os.path.join(output_dir, COMPLETION_FILE)
        print('result_file_name : ', result_file_name)
        completed = os.path.exists(result_file_name) and os.path.isfile(result_file_name) and os.stat(
            result_file_name).st_size != 0
    except Exception as e:
        print('flo2d_model_completed|Exception : ', str(e))
    finally:
        return completed
