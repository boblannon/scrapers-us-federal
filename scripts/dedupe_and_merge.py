import json
from scripts import global_dedupe, merge_dupes


def main():
    while True:
        dedupe_record_loc = global_dedupe.main()
        with open(dedupe_record_loc) as dedupe_record:
            merges = json.load(dedupe_record)
        
        # If echelon doesn't recommend any merges, stop
        if len(merges) == 0:
            break

        num_deleted = merge_dupes.main()

if __name__ == '__main__':
    main()
