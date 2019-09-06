# Python 3.7.2
from datetime import datetime

from gooey import Gooey, GooeyParser
from pandas import read_excel, read_csv, DataFrame, errors

now = datetime.now()

# TODO: Retrieve Job Number
# TODO: Comment code.
# TODO: Add print statements to indicate progress. ('Merging files now...')
"""
Checks for full 4-column Contract Number - If it doesnt exist, reduces to two column Contract Number.
Drops duplicate Contract Numbers in branding grid.
"""


class AnthemMerge:
    def __init__(self, brandgrid, maillist):
        try:
            self._dfGrid = read_excel(brandgrid, dtype=str)
        except errors.ParserError:
            print("Unable to import Branding Grid")
        try:
            self._dfList = read_csv(maillist, dtype=str)
        except errors.ParserError:
            print("Unable to process Mailing List")

        # Cleanup Branding Grid
        self._dfGrid.columns = self._dfGrid.columns.str.title().str.strip()
        self._dfGrid = self._dfGrid.fillna('')
        self._dfGrid['Contract Number'] = self._dfGrid.apply(col_merge, axis=1)

        # Cleanup Mailing List
        self._dfList.columns = self._dfList.columns.str.title()
        self._dfList['Contract Number'] = self._dfList.apply(self.col_check, axis=1)

        # initialize DataFrames
        self.dfMerged = DataFrame()
        self.dfProofs = DataFrame()
        self.dfFinal = DataFrame()

    def col_check(self, row):
        """

        :rtype: object
        """
        if row['List Contract Number'] in row[self._dfGrid['Contract Number']]:
            return row['List Contract Number']
        else:
            return row['List Contract Number'][0:9]

    def merge(self):
        print('Merging files...')
        self.dfMerged = self._dfList.join(self._dfGrid.drop_duplicates(['Contract Number']).set_index('Contract Number'),
                                          on='Contract Number')
        self.dfFinal = self.dfMerged  # set to dfFinal for when proofs aren't being made.

    def get_proofs(self):
        """ Generate a new data frame consisting of the first 2 records
            of each unique contract number
        """
        print('Getting unique Contract Numbers for proofs..')
        contracts = self.dfMerged['Contract Number'].unique()
        for x in range(len(contracts)):
            self.dfProofs = self.dfProofs.append(self.dfMerged.loc[self.dfMerged['Contract Number'] ==
                                                                   contracts[x]].head(2))
        self.dfProofs['Proofs'] = 'Proof'
        print('Adding proofs to mailing list...')
        self.dfFinal = self.dfProofs.append(self.dfMerged, ignore_index=True, sort=False)

    def create_csv(self):
        """ Output to csv file with Latin(ISO-8859-1) encoding for compatibility with Variable Data Software
        """
        name = f'Anthem Merged_{now:%m%d%y}'
        print('Outputting list to .csv...')
        self.dfFinal.to_csv(name + '.csv', index=False, encoding='ISO-8859-1')


def col_merge(row):
    contract_name = []
    if row['Contract 1']:
        contract_name.append(row['Contract 1'])
    if row['Contract 2']:
        contract_name.append(row['Contract 2'])
    if row['Sourcegroupnumber']:
        contract_name.append(row['Sourcegroupnumber'])
    if row['Sourcesubgrpnbr']:
        contract_name.append(row['Sourcesubgrpnbr'])
    return '-'.join(contract_name)


@Gooey(program_name='Anthem Merge Program')
def main():
    parser = GooeyParser(description='Combine the branding grid with a processed mailing list for variable data merge')
    parser.add_argument('Branding_Grid',
                        help="Select the Branding Grid File (.xlsx)",
                        widget="FileChooser")
    parser.add_argument('Mailing_List',
                        help="Select the Mailing List File (.csv)",
                        widget="FileChooser")
    args = parser.parse_args()
    anthemjob = AnthemMerge(args.Branding_Grid, args.Mailing_List)
    anthemjob.merge()
    # anthemjob.get_proofs()
    anthemjob.create_csv()
    del anthemjob
    print('Job Complete!')


if __name__ == '__main__':
    main()
