# Python 3.7.2
import datetime
from pprint import pprint

from gooey import Gooey, GooeyParser
from pandas import read_excel, read_csv, DataFrame, errors, pivot_table

now = datetime.datetime.now()

# TODO: implement try/except blocks for file handling
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
        self.dfMerged = DataFrame()
        self.dfProofs = DataFrame()
        self.dfFinal = DataFrame()

    def create_contract(self):
        """ Prepare data frames so both have a Contract Number column for indexing
        """
        self._dfGrid.insert(column="Contract Number",
                            loc=2,
                            value=self._dfGrid['Contract 1'] +
                            '-' + self._dfGrid['Contract 2'])
        self._dfGrid.drop(['Contract 1', 'Contract 2'],
                          axis=1,
                          inplace=True)
        self._dfList.rename(columns={'List Contract Number': 'Contract Number'},
                            inplace=True)

    def merge(self):
        self.dfMerged = self._dfList.join(self._dfGrid.set_index('Contract Number'),
                                          on='Contract Number')

        dfpivot = pivot_table(self.dfMerged, index=['Contract Number'], columns=['Envelope'], aggfunc=len)
        dfpivot.reset_index(inplace=True)

        pprint(dfpivot, width=80)



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

    del anthemjob


if __name__ == '__main__':
    main()
