import unittest   # The test framework
import re




#regex fonctions :---------------------------------------------------------------#
def cin(text):
    result = re.search(r'(?<=identité N°.)[A-Z][A-Z0-9][0-9]{5}', text)
    if result!=None:
        return result.group()
    else:
        print('None')

def no_compte(text):
    result =  re.search(r'[0-9]{4}[A-Z][0-9]{9}', text)
    if result!=None:
        return result.group()
    else:
        print('None')

def date_naissance(text) :
    result = re.search(r'\d{2}/\d{2}/\d{4}', text)
    if result!=None:
        return result.group()
    else:
        print('None')

def tel(text) :
    result = re.search(r'06[0-9]{8}', text)
    if result!=None:
        return result.group()
    else:
        print('None')

def name(text):
   tel = re.search('(?<=assurer :)(.*[A-Z\s]?)(?=Pièce)', text)
   if(tel==None):print('None')
   else :return re.match('(|[A-Z\s]+)(.|\s+)([A-Z\s]+)', tel.group()).group()

def File_path(text):
    return text.replace('file:/','').replace('output','input').replace('txt','pdf')
#---------------------------------------------------------------------------------------------#

class Test_CIN(unittest.TestCase):
    def test_cin_1(self):
        result = cin("identité N°.BK29706")
        self.assertNotEqual(result, "BK297069")
    def test_cin2(self):
        result = cin("identité N°.BK29706")
        self.assertEqual(result,"BK29706")

class Test_TEL(unittest.TestCase):
    def test_tel_1(self):
        result = tel("0620542106 c'est mon numero !")
        self.assertEqual(result,'0620542106')
    def test_tel_2(self):
        result = tel("0620542106 c'est mon numero !")
        self.assertNotEqual(result,'0020542106')

class Test_NO_COMPTE(unittest.TestCase):
    def test_noCompte_1(self):
        result = no_compte("MON COMPTE:4235G134256789")
        self.assertEqual(result,'4235G134256789')
    def test_noCompte_2(self):
        result = no_compte("MON COMPTE:4235G134256789")
        self.assertNotEqual(result,'42351134256789')

class Test_DATE_NAISSANCE(unittest.TestCase):
    def test_dateNaissance_1(self):
        result = date_naissance("ma date de naissance :04/07/1996")
        self.assertEqual(result,'04/07/1996')
    def test_date_naissance_2(self):
        result = date_naissance("ma date de naissance :04-07-1996")
        self.assertNotEqual(result,'04/07/1996')

class Test_NAME(unittest.TestCase):
    def test_name_1(self):
        result = name("assurer : GOUALI ABDELLATIF .Pièce")
        self.assertEqual(result,' GOUALI ABDELLATIF ')
    def test_name_2(self):
        result = name("assurer : GOUALI ABDELLATIF Pièce")
        self.assertNotEqual(result,'GOUALI ABDELLATIF ')
class Test_File_Path(unittest.TestCase):
    def test_file_path_1(self):
        result = File_path("file:/D:output/file.txt")
        self.assertEqual(result,'D:input/file.pdf')
    def test_file_path_2(self):
        result = File_path("file:/D:output/file.txt")
        self.assertNotEqual(result,'D:output/file.pdf')

if __name__ == '__main__':

    unittest.main()