# Python 3.7.2
from datetime import datetime
from os import path
from sys import exit

from gooey import Gooey, GooeyParser
from pandas import read_excel, read_csv, DataFrame, errors

now = datetime.now()

# TODO: Retrieve Job Number
# TODO: Comment code.


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
        self._dfGrid.insert(column="Contract Number",
                            loc=2,
                            value=self._dfGrid['Contract 1'] +
                            '-' + self._dfGrid['Contract 2'])
        self._dfGrid.drop(['Contract 1', 'Contract 2'],
                          axis=1,
                          inplace=True)
        self.dfDupes = self._dfGrid[self._dfGrid.duplicated(['Contract Number'])]
        if not self.dfDupes.empty:
            self.dfDupes.to_csv(path.basename(brandgrid)[:-5] + " duplicates.csv")
            exit('BRANDING GRID HAS DUPLICATES! DO NOT USE')

        self._dfList.rename(columns={'List Contract Number': 'Contract Number'},
                            inplace=True)

        self.dfMerged = DataFrame()
        self.dfProofs = DataFrame()
        self.dfFinal = DataFrame()

    def merge(self):
        self.dfMerged = self._dfList.join(self._dfGrid.set_index('Contract Number'),
                                          on='Contract Number')

    def get_proofs(self):
        """ Generate a new data frame consisting of the first 2 records
            of each unique contract number
        """
        contracts = self.dfMerged['Contract Number'].unique()
        for x in range(len(contracts)):
            self.dfProofs = self.dfProofs.append(self.dfMerged.loc[self.dfMerged['Contract Number'] ==
                                                                   contracts[x]].head(2))
        self.dfProofs['Proofs'] = 'Proof'
        self.dfFinal = self.dfProofs.append(self.dfMerged, ignore_index=True, sort=False)

    def create_csv(self):
        """ Output to csv file with Latin(ISO-8859-1) encoding for compatibility with Variable Data Software
        """
        name = f'Anthem Merged_{now:%m%d%y}'
        self.dfFinal.to_csv(name + '.csv', index=False, encoding='ISO-8859-1')


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
    anthemjob.get_proofs()
    anthemjob.create_csv()
    del anthemjob


if __name__ == '__main__':
    main()
