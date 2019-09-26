# Python 3.7.2
from datetime import datetime
from os import path

from gooey import Gooey, GooeyParser
from pandas import read_excel, read_csv, errors

now = datetime.now()

# TODO: Retrieve Job Number
# TODO: Comment code.
# TODO: Add checkbox for outputting split .csv files
# TODO: Add print statements to indicate progress. ('Merging files now...')


class AnthemMerge:
    def __init__(self, brandgrid: object, maillist: object):
        """
        Purpose: Initialize a branding grid and a mailing list for merging via a left join.
        :type brandgrid: object
        :type maillist: object
        """
        try:
            self._dfGrid = read_excel(brandgrid, dtype=str)
        except errors.ParserError:
            print("Unable to import Branding Grid")
        try:
            self._dfList = read_csv(maillist, dtype=str)
        except errors.ParserError:
            print("Unable to process Mailing List")

        # Cleanup Branding Grid
        self._dfGrid.columns = self._dfGrid.columns.str.title()
        self._dfGrid.columns = self._dfGrid.columns.str.strip()

        self._dfGrid.insert(column="Contract Number",
                            loc=2,
                            value=self._dfGrid['Contract 1'] +
                            '-' + self._dfGrid['Contract 2'])
        self._dfGrid.drop(['Contract 1', 'Contract 2'],
                          axis=1,
                          inplace=True)

        self._dfDupes = self._dfGrid[self._dfGrid.duplicated(['Contract Number'])]
        if not self._dfDupes.empty:
            self._dfDupes.to_csv(path.basename(brandgrid)[:-5] + " duplicates.csv")
            exit('BRANDING GRID HAS DUPLICATES! DO NOT USE')
        self._dfGrid = self._dfGrid[['Contract Number', 'Envelope']]

        self._dfList.columns = self._dfList.columns.str.title()
        self._dfList.rename(columns={'List Contract Number': 'Contract Number'},
                            inplace=True)

    def merge(self):
        df_merged = self._dfList.join(self._dfGrid.set_index('Contract Number'),
                                      on='Contract Number')
        # Output new .csv files based on Envelope type.
        dfs = dict(tuple(df_merged.groupby(['Envelope'])))
        listdf = [dfs[x] for x in dfs]
        for idx, frame in enumerate(listdf):
            listdf[idx].to_csv(listdf[idx].iloc[0]['Envelope'] + f' Envelope List_{now:%m%d%y}.csv',
                               index=False,
                               header=True)

        # Output breakdown of merged list based on Envelope type and Contract Number
        # Fill na values with #N/A to indicate missing values from join.
        df_group = df_merged.fillna('#N/A').groupby(['Envelope', 'Contract Number'])
        df_group['Envelope'].agg(len).to_csv(f'Merge Summary_{now:%m%d%y}.csv', header=True)


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

    del anthemjob


if __name__ == '__main__':
    main()
