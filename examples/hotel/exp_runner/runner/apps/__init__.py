from .hotel import HotelApp


def get_app_plugin(name):
    if name != "hotel":
        raise ValueError(f"Unsupported application: {name}")
    return HotelApp()
