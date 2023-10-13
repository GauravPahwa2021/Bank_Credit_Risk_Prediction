from setuptools import find_packages,setup
from typing import List

HYPEN_E_DOT = '-e .'

def get_requirements(file_path:str)->List[str]:
    '''This function will return list of requirements from requirements.txt file'''
    requirements = []
    with open(file_path) as file_obj:
        requirements = file_obj.readlines()
        requirements = [req.replace("\n"," ") for req in requirements]

        # Remove hyphen_e_dot if present in requirements
        if HYPEN_E_DOT in requirements:
            requirements.remove(HYPEN_E_DOT)

    return requirements

setup(
    name = 'ML_Project_Bank_Credit_Risk_Prediction',
    version = '0.0.1',
    author = 'Gaurav Pahwa',
    author_email = '2020uce1603@mnit.ac.in',
    packages = find_packages(),
    install_requires = get_requirements('requirements.txt')
)