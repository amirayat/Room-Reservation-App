from django.db.models import Q
from django.db import models
from core.base_model import RootModel


class Room(RootModel):
    """
    model class for rooms
    """
    ROOM_TYPE = (
        ("Single", "Single"),
        ("Double", "Double"),
        ("Triple", "Triple"),
        ("Quad", "Quad"),
        ("VIP", "VIP")
    )
    type = models.CharField(choices=ROOM_TYPE, max_length=6)
    price = models.PositiveIntegerField()

    def __str__(self):
        return str(self.id)

    class Meta:
        db_table = 'room'


class ReservationQuerySet(models.query.QuerySet):
    """
    custom QuerySet for Reservation model
    """

    def find_reservations(self, from_dt, to_dt):
        """
        find reservations for a given period of time 
        """
        cond1 = Q(from_date__range=[from_dt, to_dt])
        cond2 = Q(to_date__range=[from_dt, to_dt])
        cond3 = Q(from_date__lte=from_dt, to_date__gte=to_dt)
        return self.filter(cond1 | cond2 | cond3)


class ReservationManager(models.manager.BaseManager.from_queryset(ReservationQuerySet)):
    """
    custom manager for Reservation model
    """

    def get_queryset(self):
        return super().get_queryset().filter(is_deleted=False)


class Reservation(RootModel):
    """
    model class for users reservations
    """
    from_date = models.DateField()
    to_date = models.DateField()
    room = models.ForeignKey(Room, on_delete=models.CASCADE)
    reservationist = models.CharField(max_length=50)
    phone = models.CharField(max_length=13)

    objects = ReservationManager()

    def __str__(self):
        return f"room {self.room_id}| {self.from_date} - {self.to_date}"

    class Meta:
        db_table = 'reservation'


