# Python 3.7.2
import datetime

from gooey import Gooey, GooeyParser
from pandas import read_excel, read_csv, errors

now = datetime.datetime.now()

# TODO: Retrieve Job Number
# TODO: Comment code.


class AnthemMerge:
    def __init__(self, brandgrid, maillist):
        # Import branding grid and mailing list.
        try:
            self._dfGrid = read_excel(brandgrid, dtype=str)
        except errors.ParserError:
            print("Unable to import Branding Grid")
        try:
            self._dfList = read_csv(maillist, dtype=str)
        except errors.ParserError:
            print("Unable to process Mailing List")

    def create_contract(self):
        """ Prepare data frames so both have a Contract Number column
            for join on common index.
        """
        # Prepare Branding Grid for merge. Reduce to Contract Number and Envelope columns
        self._dfGrid.columns = self._dfGrid.columns.str.title()
        self._dfGrid.columns = self._dfGrid.columns.str.strip()
        self._dfGrid.insert(column='Contract Number',
                            value=self._dfGrid['Contract 1'] +
                            '-' + self._dfGrid['Contract 2'])
        self._dfGrid = self._dfGrid[['Contract Number', 'Envelope']]

        # Prepare mailing list for merge. Name Contract Number column.
        self._dfList.columns = self._dfList.columns.str.title()
        self._dfList.rename(columns={'List Contract Number': 'Contract Number'},
                            inplace=True)

    def merge(self):
        dfMerged = self._dfList.join(self._dfGrid.set_index('Contract Number'),
                                          on='Contract Number')
        # Output new .csv files based on Envelope type.
        dfs = dict(tuple(dfMerged.groupby(['Envelope'])))
        listdf = [dfs[x] for x in dfs]
        for idx, frame in enumerate(listdf):
            listdf[idx].to_csv(listdf[idx].iloc[0]['Envelope'] + f' Envelope List_{now:%m%d%y}.csv',
                               index=False,
                               header=True)

        # Output breakdown of merged list based on Envelope type and Contract Number
        # Fill na values with #N/A to indicate missing values from join.
        dfgroup = dfMerged.fillna('#N/A').groupby(['Envelope', 'Contract Number'])
        dfgroup['Envelope'].agg(len).to_csv(f'Merge Summary_{now:%m%d%y}.csv', header=True)


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
