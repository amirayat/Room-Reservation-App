import pandas as pd
from collections import OrderedDict
from datetime import date, datetime
from rest_framework.exceptions import NotAcceptable
from .models import Reservation, Room
from .serializers import ReservationSerializer


def expand_reservations(reservations: list) -> pd.DataFrame:
    """
    expand reservations to date units in data frame structure
    """
    df = pd.DataFrame(reservations,
                      columns=['room', 'from_date', 'to_date'])
    # melt from_date and to_date to single column
    df_melted = pd.melt(df,
                        id_vars=['room'],
                        value_vars=['from_date', 'to_date'])\
        .sort_values(by=['room', 'value'])
    df_melted.drop(columns='variable', inplace=True)
    df_melted.rename(columns={'value': 'date'}, inplace=True)
    df_melted['date'] = pd.to_datetime(df_melted['date'])
    # groupby, resample and interpolate df to fill missed dates
    df_resampled = df_melted.groupby('room')\
        .apply(lambda df: df.set_index('date')
               .resample('D')
               .first()
               .interpolate())
    df_resampled = df_resampled.reset_index(level=0, drop=True).reset_index()
    df_resampled = df_resampled.astype({"room": int})
    return df_resampled


def exist_reservations(from_date: date, to_date: date, room_id=None) -> pd.DataFrame:
    """
    return exist reservations for a given date time range in data frame structure
    """
    if room_id:
        queryset = Reservation.objects.filter(room_id=room_id)\
            .find_reservations(from_date, to_date)
    else:
        queryset = Reservation.objects.find_reservations(from_date, to_date)
    _exist_reservations = ReservationSerializer(queryset, many=True).data

    df = expand_reservations(_exist_reservations)
    return df


def available_reservations(from_date: date, to_date: date, room_id=None) -> list:
    """
    return available reservations for a given date time range
    """
    if room_id:
        rooms = [int(room_id)]
    else:
        rooms = [room['id'] for room in Room.objects.values("id")]

    _exist_reservations = exist_reservations(from_date, to_date, room_id)

    # create a df of full reservations for a given date time range
    frames = [pd.DataFrame({
        'date': pd.date_range(
            start=from_date,
            end=to_date
        ),
        'room': room
    }) for room in rooms]
    _full_reservations = pd.concat(frames)

    if not _exist_reservations.empty:
        # subtract two dfs
        df = pd.concat([
            _full_reservations,
            _exist_reservations
        ]).drop_duplicates(keep=False)
    else:
        df = _full_reservations

    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    return df.to_dict('records')


def reservation_checker(data, many):
    """
    Checks given data has coverage with exist reservations
    """
    if not many:
        data = [data]
    under_check_reservations = [
        OrderedDict(
            [
                ('from_date', reservation["from_date"]),
                ('to_date', reservation["to_date"]),
                ('room', int(reservation["room"]))
            ]
        ) for reservation in data
    ]
    under_check_reservations_df = expand_reservations(under_check_reservations)

    # bring all exist reservation between wided date time range
    from_date = datetime.strptime(
        min(under_check_reservations, key=lambda x: x['from_date'])['from_date'], '%Y-%m-%d').date()
    to_date = datetime.strptime(
        max(under_check_reservations, key=lambda x: x['to_date'])['to_date'], '%Y-%m-%d').date()
    _exist_reservations_df = exist_reservations(from_date, to_date)

    if _exist_reservations_df.empty:
        return
    
    # find two dfs intersection
    df = pd.merge(
        under_check_reservations_df,
        _exist_reservations_df,
        on=['room','date'],
        how='inner'
    )
    print(df)
    if not df.empty:
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        raise NotAcceptable(
            {"exist_reservations": df.to_dict('records')})
