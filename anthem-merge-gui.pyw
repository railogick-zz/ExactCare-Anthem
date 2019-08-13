# Python 3.7.2
import datetime
from pandas import read_excel, read_csv, DataFrame
from gooey import Gooey, GooeyParser

now = datetime.datetime.now()

# TODO: Change import based on file type.
# TODO: Cleanup the interface (switch to tkinter?)
# TODO: Retrieve Job Number
# TODO: Comment code.


class AnthemMerge:
    def __init__(self, brandgrid, maillist):
        self.dfGrid = read_excel(brandgrid, dtype=str)
        self.dfList = read_csv(maillist, dtype=str)
        self.dfMerged = DataFrame()
        self.dfProofs = DataFrame()
        self.dfFinal = DataFrame()

    def create_contract(self):
        """ Prepare data frames so both have a Contract Number column for indexing
        """
        self.dfGrid.insert(loc=2,
                           column="Contract Number",
                           value=self.dfGrid['Contract 1'] +
                           '-' + self.dfGrid['Contract 2'])
        self.dfGrid.drop(['Contract 1', 'Contract 2'],
                         axis=1,
                         inplace=True)
        self.dfList.rename(columns={'List Contract Number': 'Contract Number'},
                           inplace=True)

    def merge(self):
        self.dfMerged = self.dfList.join(self.dfGrid.set_index('Contract Number'),
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
        """ Output to csv file with ISO-8859-1 encoding for compatibility with Variable Data Software
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
    anthemjob.create_contract()
    anthemjob.merge()
    anthemjob.get_proofs()
    anthemjob.create_csv()
    del anthemjob


if __name__ == '__main__':
    main()
