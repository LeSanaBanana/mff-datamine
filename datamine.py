import os
import json
import csv
import base64
import heapq
import multiprocessing as mp
import requests
import io
import re

import UnityPy
from shutil import copyfile
from PIL import Image
import csv

from time import time
from colorama import init, Fore, Style
import terminaltables
import argparse
from textwrap import wrap

# Files are stored in:
# ./_Game Files/{version_num}/{filename}
# Previous version should not have alphas

# Run "python dataminer.py {version_to_datamine} {previous_version} all"

class Unpack:
    def __init__(self, current, past, reformat):

        self.current = current
        self.past = past
        self.report = {
            'newfiles': []
        }
        init() # colorama

        # Convert to unity3d
        os.chdir(f"./_Game Files/{current}")
        if reformat == "yes":
            for thing in os.listdir('.'):
                if 'ui_' in thing or "newcontents" in thing and '.' not in thing:
                    print(Fore.GREEN + Style.BRIGHT +
                          f"Formatting file {thing}" + Style.RESET_ALL)
                    os.rename(thing, thing + ".unity3d")
        elif reformat == "nc":
            for thing in os.listdir('.'):
                if "newcontents" in thing and '.' not in thing:
                    print(Fore.GREEN + Style.BRIGHT +
                          f"Formatting file {thing}" + Style.RESET_ALL)
                    os.rename(thing, thing + ".unity3d")
        elif reformat == "all":
            for thing in os.listdir('.'):
                if ".csv" not in thing:
                    print(Fore.GREEN + Style.BRIGHT +
                          f"Formatting file {thing}" + Style.RESET_ALL)
                    os.rename(thing, thing + ".unity3d")

        # Find unity files with no folders already
        unities = []
        newcontents = []
        for thing in os.listdir('.'):
            if thing.endswith('.unity3d'):
                if thing.replace('.unity3d', '') not in os.listdir('.'):
                    if 'newcontents' not in thing.lower():
                        unities.append(thing)
                    else:
                        newcontents.append(thing)
        self.report['files'] = [x[:-8] for x in unities]

        p = mp.Pool()
        p.map(self.unpack, unities)

        # Look for "new contents" files
        os.chdir(f'./../{current}')
        if len(newcontents) > 0 and 'NewContents' not in os.listdir('.'):
            print(Fore.MAGENTA + Style.BRIGHT +
                  "Beginning new contents extraction" + Style.RESET_ALL)
            print(f"[unpacking] Creating folder NewContents")
            os.mkdir('NewContents')
            self.report['files'] = self.report['files'] + [x[:-8] for x in newcontents]
            for newcontent in newcontents:
                self.extract_nc(newcontent)

        # Look for new skills
        if "_skillreport.txt" not in os.listdir('.'):
            print(Fore.MAGENTA + Style.BRIGHT +
                "Beginning skill comparison" + Style.RESET_ALL)
            self.new_skills()

        # Compare localisation files
        if "_newstrings.txt" not in os.listdir('.'):
            print(Fore.MAGENTA + Style.BRIGHT +
                "Beginning localisation file comparison" + Style.RESET_ALL)
            self.compare_localisation()

        self.make_report()

    def unpack(self, filename):
        """Unpack the unity file"""

        print(Fore.MAGENTA + Style.BRIGHT +
                f"Beginning unpack for file {filename}" + Style.RESET_ALL)

        env = UnityPy.load(filename)

        # Create folder
        foldername = filename.replace('.unity3d', '')
        print(f"[unpacking] Creating folder {foldername}")
        os.mkdir(foldername)

        # Unpack files
        print(f"[unpacking] Starting unpacking")
        for obj in env.objects:
            # Images
            if obj.type.name in ['Texture2D', 'Sprite']:
                data = obj.read()
                dest = os.path.join(f"./{foldername}",
                                    data.name)  # output destination
                dest, ext = os.path.splitext(dest)
                dest = dest + ".png"
                img = data.image
                img.save(dest)
                
            # JSON data files
            elif obj.type.name == 'MonoBehaviour':
                if obj.serialized_type.nodes:
                    # save decoded data
                    tree = obj.read_typetree()
                    fp = os.path.join(f"./{foldername}", f"{obj.read().name}.json")
                    with open(fp, "wt", encoding = "utf8") as f:
                        json.dump(tree, f, ensure_ascii = False, indent = 4)
                else:
                    # save raw relevant data (without Unity MonoBehaviour header)
                    data = obj.read()
                    fp = os.path.join(f"./{foldername}", f"{data.name}.bin")
                    with open(fp, "wb") as f:
                        f.write(data.raw_data)
                        
            # CSV and plaintext data files
            elif obj.type.name == "TextAsset":
                data = obj.read()

                # "Bad words" - unencoded, save as plaintext
                if "BAD_WORDS" in data.name:
                    fp = os.path.join(f'./{foldername}', f"{data.name}.txt")
                    with open(fp, 'wt', encoding='utf8') as f:
                        f.write(data.text)
                        
                # "Map data" - encoded JSON, save into separate files
                elif data.name.isnumeric():
                    folder_path = os.path.join(f'./{foldername}', 'maps')
                    os.makedirs(folder_path, exist_ok=True)
                    
                    byte_string = base64.b64decode(data.text)
                    decoded_string = byte_string.decode('utf-16le')
                    fixed_json_string = re.sub(r'}\s*{', '},{', decoded_string)
                    json_data = []
                    try:
                        json_data = json.loads(fixed_json_string)
                        fp = os.path.join(f'./{foldername}', 'maps', f"{data.name}.json")
                    except json.decoder.JSONDecodeError:
                        json_data = "Failed :("
                        fp = os.path.join(f'./{foldername}', 'maps', f"FAIL_{data.name}.json")
                    with open(fp, 'wt', encoding='utf8') as f:
                        json.dump(json_data, f, indent=4)
                
                # Anything else - encoded CSV
                else:
                    fp = os.path.join(f'./{foldername}', f"{data.name}.csv")
                    byte_string = base64.b64decode(data.text)
                    decoded_string = byte_string.decode('utf-16le')
                    decoded_lines = decoded_string.split('\n')
                    decoded_data = [line.strip().split('\t') for line in decoded_lines]
                    with open(fp, 'wt', newline='', encoding='utf8') as f:
                        writer = csv.writer(f)
                        writer.writerows(decoded_data)

        print(f"[unpacking] Finished unpacking")

        self.unmask(foldername)

    def unmask(self, foldername):
        """Unmask using alphas"""

        os.chdir(f"./{foldername}")
        fail_list = []

        if foldername.lower() != "ui_skillicons":
            print(f"[unmask] Starting unmasking")
            for name in os.listdir('.'):
                if 'alpha' not in name and ".png" in name:
                    try:
                        mask = Image.open(name[:-4] +
                                          "_alpha.png").convert('L')
                        img = Image.open(name).convert('RGBA')
                        w, h = img.size
                        slate = Image.new('RGBA', (w, h))
                        slate.paste(img, (0, 0), mask)
                        slate.save(f'{name}')
                        os.remove(name[:-4] + "_alpha.png")
                    except (FileNotFoundError, ValueError):
                        fail_list.append(name)
        else:
            print(f"[unmask] Starting skill icon unmasking")
            for name in os.listdir('.'):
                if 'alpha' not in name and ".png" in name:
                    try:
                        mask = Image.open("alpha.png").convert('L')
                        img = Image.open(name).convert('RGBA')
                        w, h = img.size
                        slate = Image.new('RGBA', (w, h))

                        slate.paste(img, (0, 0), mask)
                        slate.save(f'{name}')
                    except (FileNotFoundError, ValueError):
                        fail_list.append(name)
        print("[unmask] Finished unmasking")

        # Fail list, if anything failed
        if len(fail_list) > 0:
            print("[unmask] Writing fail list")
            with open('fails.txt', 'w') as f:
                f.writelines(["%s\n" % item for item in fail_list])

        self.find_new(foldername)

        os.chdir('./..')  # reset for next file

    def find_new(self, foldername):
        """Find new files compared to old
        Remove function call if not relevant"""

        # If no old folder, return
        if foldername not in os.listdir(f'./../../{self.past}/'):
            print(
                f"[find_new] Couldn't find a folder {foldername} for version {self.past}"
            )
            return

        # If old folder, get list of olds and add to dict
        print("[find_new] Indexing previous version files")
        old_files = os.listdir(f'./../../{self.past}/{foldername}')
        existing_dict = {}
        for old in old_files:
            existing_dict[old.replace('-', '_')] = 1

        # Compare to new files
        print("[find_new] Copying new files")
        for new in os.listdir('.'):
            try:
                existing_dict[new.replace('-', '_')]
            except KeyError:
                try:
                    # If new file detected, make folder and add to report dict
                    if not os.path.exists('new'):
                        os.mkdir('new')
                        make_folder = True
                    if foldername not in self.report['newfiles']:
                        self.report['newfiles'].append(foldername)
                    copyfile(new, f'./new/{new}')
                except PermissionError:  # why
                    pass
        print('[find_new] Copied all new files to "new" directory')

    def extract_nc(self, filename):
        """Extracts and resizes "new contents" files."""

        env = UnityPy.load(filename)

        # Unpack images
        for obj in env.objects:
            if obj.type.name in ['Texture2D', 'Sprite']:
                data = obj.read()
                dest = os.path.join(f"./NewContents",
                                    data.name)  # output destination
                dest, ext = os.path.splitext(dest)
                dest = dest + ".png"
                img = data.image
                # img = img.resize((1028, 512))
                img.save(dest)
        print(f"[unpacking] Finished resizing & unpacking {filename}")

    def new_skills(self):
        """Find new skills"""

        # Load json into dicts
        with open("text/HERO_SKILL.json") as f:
            new = json.load(f)
        with open(f"../{self.past}/text/HERO_SKILL.json") as f:
            old = json.load(f)

        # Load and format chars.json from thanosvibs
        url = "https://thanosvibs.money/static/data/chars.json"
        response = requests.get(url)
        if response.status_code == 200:
            bytes_io = io.BytesIO(response.content)
            json_str = bytes_io.read().decode('utf-8')
            json_data = json.loads(json_str)
        else:
            print(f"Failed to retrieve chars.json: {response.status_code}")
        chars = {str(x['id']): x['character'] for x in json_data if x['uniformed'] == 'False'}

        print("[skills] Loaded skill files into dicts")

        # Variables are now lists of dicts
        new = new['values']
        old = old['values']

        # Create dict from old values
        existing = {}
        for skill in old:
            existing[json.dumps(skill, sort_keys=True)] = 1
        print("[skills] Existing skill dict created")

        # Check each new skill
        new_skill_count = 0
        new_skill_chars = {}
        new_t3_values = []
        for skill in new:
            try:
                existing[json.dumps(skill, sort_keys=True)]
            except KeyError:
                new_skill_count += 1

                # Check the hero the skill belongs to
                if 'heroGroupId' in skill.keys():
                    try:
                        hero = chars[str(skill['heroGroupId'])]
                        if hero not in new_skill_chars.keys():
                            new_skill_chars[hero] = 1
                        else:
                            new_skill_chars[hero] += 1

                        # Check if DRX/T3 values are the only changes
                        skill["ultimateSkillGauge"] = 0
                        skill["dangerSkillGauge"] = 0
                        try:
                            existing[json.dumps(skill, sort_keys=True)]
                            if hero not in new_t3_values:
                                new_t3_values.append(hero)
                        except KeyError:
                            pass
                    except KeyError:
                        print(f"Unknown hero group id: {skill['heroGroupId']}")

        # Terminal output
        print(f"[skills] Identified {new_skill_count} new skills for {', '.join(new_skill_chars.keys())}")
        if len(new_t3_values) > 0:
            print(f"[skills] Identified {len(new_t3_values)} new T3/TP variables for {', '.join(new_t3_values)}")

        # Add to report dict
        self.report['t3tpchars'] = ', '.join(new_t3_values)
        self.report['skillcount'] = new_skill_count
        self.report['skillchars'] = ', '.join(new_skill_chars.keys())

        # Generate report .txt, also determines if we re-run
        new_skill_chars = [f"{x}: {new_skill_chars[x]}" for x in new_skill_chars.keys()]
        new_skill_pairs = '\n'.join(new_skill_chars)
        text = f"NEW SKILLS: {new_skill_count}\n{new_skill_pairs}\n\nNEW T3/TP: {len(new_t3_values)}\n{', '.join(new_t3_values)}"
        with open("_skillreport.txt", 'w', encoding='utf8') as f:
            f.write(text)

    def compare_localisation(self):
        """Compare localisation files"""
        
        # Load json into dicts
        with open('localization_en/Localization_en.json', encoding='utf8') as f:
            new = json.load(f)
        with open(f'../{self.past}/localization_en/Localization_en.json', encoding='utf8') as f:
            old = json.load(f)
        print("[localisation] Loaded localisation files into dicts")

        # Variables are now lists of strings
        new = new['valueTable']['values']
        old = old['valueTable']['values']

        # Create dicts
        newdict = {}
        olddict = {}
        for i, string in enumerate(new):
            newdict[string] = i
        for i, string in enumerate(old):
            olddict[string] = i
        print('[localisation] Dictioraries of old and new dicts built')
        
        # Set operations
        added = list(set(new) - set(old))
        removed = list(set(old) - set(new))

        # Sort by place in original dict
        added_sorted = []
        for string in added:
            heapq.heappush(added_sorted, (newdict[string], string))
        removed_sorted = []
        for string in removed:
            heapq.heappush(removed_sorted, (olddict[string], string))

        # Add to report dict
        self.report['added'] = len(added)
        self.report['removed'] = len(removed)

        # Output
        with open("_newstrings.txt", 'w', encoding='utf8') as f:
            while added_sorted:
                f.write(heapq.heappop(added_sorted)[1] + "\n")
        with open("_oldstrings.txt", 'w', encoding='utf8') as f:
            while removed_sorted:
                f.write(heapq.heappop(removed_sorted)[1] + "\n")

    def make_report(self):
        """Generate a report for the datamine job"""

        # Get report number
        index = 0
        while f"_report{index}.txt" in os.listdir('.'):
            index += 1

        # Make list of lists for table
        table_data = [
            ['version', self.current]
            # ['bundles', self.report['files']],
            # ['altered bundles', ', '.join(self.report['newfiles'])]
            # ['new strings', self.report['added']],
            # ['removed strings', self.report['removed']],
            # ['new skill count', self.report['skillcount']],
            # ['new skill chars', self.report['skillchars']],
            # ['new t3/tp chars', self.report['t3tpchars']]
        ]
        index_tracker = 1
        if len(self.report['files']) > 0:
            file_index = index_tracker
            index_tracker += 1
            file_string = ', '.join(self.report['files'])
            table_data.append(['bundles', ''])
        if 'newfiles' in self.report.keys() and len(self.report['newfiles']) > 0:
            newfile_index = index_tracker
            index_tracker += 1
            newfile_string = ', '.join(self.report['newfiles'])
            table_data.append(['altered bundles', ''])
        if 'added' in self.report.keys():
            index_tracker += 2
            table_data.append(['new strings', self.report['added']])
            table_data.append(['removed strings', self.report['removed']])
        if 'skillcount' in self.report.keys() and self.report['skillcount'] > 0:
            skill_index = index_tracker + 1
            index_tracker += 2
            table_data.append(['new skill count', self.report['skillcount']])
            table_data.append(['new skill chars', ''])
        if 't3tpchars' in self.report.keys() and len(self.report['t3tpchars']) > 0:
            t3tp_index = index_tracker
            table_data.append(['new t3/tp chars', self.report['t3tpchars']])

        # Initialise table
        if len(table_data) > 0:
            table = terminaltables.AsciiTable(table_data, 'datamine results')

            # Calculate newlines
            max_width = table.column_max_width(1)
            if len(self.report['files']) > 0:
                wrapped_string = '\n'.join(wrap(file_string, max_width))
                table.table_data[file_index][1] = wrapped_string
            if 'newfiles' in self.report.keys() and len(self.report['newfiles']) > 0:
                wrapped_string = '\n'.join(wrap(newfile_string, max_width))
                table.table_data[newfile_index][1] = wrapped_string
            if 'skillcount' in self.report.keys() and self.report['skillcount'] > 0:
                wrapped_string = '\n'.join(wrap(self.report['skillchars'], max_width))
                table.table_data[skill_index][1] = wrapped_string
            if 't3tpchars' in self.report.keys() and len(self.report['t3tpchars']) > 0:
                wrapped_string = '\n'.join(wrap(self.report['t3tpchars'], max_width))
                table.table_data[t3tp_index][1] = wrapped_string

            print(table.table)

            # Write to text file
            with open(f"_report{index}.txt", 'w', encoding='utf8') as f:
                f.write(table.table)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('current_version', nargs=1, type=str)
    parser.add_argument('previous_version', nargs=1, type=str)
    parser.add_argument('format', nargs=1, type=str)
    args = parser.parse_args()
    start = time()
    Unpack(args.current_version[0], args.previous_version[0], args.format[0])
    end = time()
    print(
        Fore.GREEN + Style.BRIGHT +
        f"Finished processing v{args.current_version[0]} in {end - start} seconds"
    )
    
