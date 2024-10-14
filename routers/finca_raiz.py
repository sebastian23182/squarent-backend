import os

import httpx

import json

from redis.asyncio import Redis

from fastapi import APIRouter

from fastapi.responses import StreamingResponse

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from apscheduler.schedulers.background import BackgroundScheduler

LOCATIONS = (
    ( 'BELLO', [6.333991, -75.554813], '9feb0402-fc35-4538-8ca1-d53c0fec2c35', 'city-colombia-05-088' ),
    ( 'MEDELLÍN', [6.249816589298594, -75.57786065131165], '183f0a11-9452-4160-9089-1b0e7ed45863', 'city-colombia-05-001' ),
    ( 'ENVIGADO', [6.166891, -75.582766], '596f30cb-3582-416e-a071-71634190a703', 'city-colombia-05-266' ),
    ( 'ITAGUÍ', [6.175069444446771, -75.61224929212936], 'cf5dc27a-9e05-4b0a-b98a-0715fe4e5d2b', 'city-colombia-05-360' ),
    ( 'SABANETA', [6.150848, -75.615552], '241a17ef-3aa0-485c-93aa-689fc2f2d114', 'city-colombia-05-631' ),
    ( 'LA ESTRELLA', [6.145162, -75.637076], 'c19b4e81-f003-408a-b7db-4bbcb9b3b6d5', 'city-colombia-05-380' ),
    ( 'CALDAS', [6.092031757719933, -75.63279850494914], '5499b608-1002-43a3-9215-01c40ffae22b', 'city-colombia-05-129' ),
    ( 'COPACABANA', [6.348654, -75.509309], 'b8f0f380-18c4-49ee-a6b3-92b454846718', 'city-colombia-05-212' ),
    ( 'GIRARDOTA', [6.379487, -75.444235], '0affff3e-ec6a-421e-a2d4-8b198493cff9', 'city-colombia-05-308' ),
)

router = APIRouter()

scheduler = BackgroundScheduler()

async def update_cache():
    #Con el iter_lines de la libreria requests se recorre cada linea de streaming mientras que iter_content espera a que todo el content este listo
    rd = Redis.from_url(url=os.getenv('REDIS_URL'))
    URL = os.getenv('FR_URL')
    for line in LOCATIONS:
        data = []
        body = {
            'variables': {
                'rows': 8000,
                'params': {
                    'page': 1,
                    'order': 3,
                    'operation_type_id': 2,
                    'property_type_id': [1, 2],
                    'locations': [
                        {
                            'country': [
                                 {
                                    'name': 'Colombia',
                                    'id': '858656c1-bbb1-4b0d-b569-f61bbdebc8f0',
                                    'slug': 'country-48-colombia'
                                }
                            ],
                            'name': line[0],
                            'location_point': {
                                'coordinates': [line[1][1], line[1][0]],
                                'type': 'point'
                            },
                            'id': line[2],
                            'type': 'CITY',
                            'slug': line[3],
                            'estate': {
                                'name': 'Antioquia',
                                'id': '2d63ee80-421b-488f-992a-0e07a3264c3e',
                                'slug': 'state-colombia-05-antioquia'
                            }
                        }
                    ],
                    'currencyID': 4,
                    'm2Currency': 4,
                    'allowMapInfo': True
                },
                'page': 1,
                'source': 10
            },
            'query': ''
        }    
                
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(URL, json = body, timeout = 100.0)
                model = Model(**response.json())
                
                for hit in model.hits.hits:
                    extracted_data = {
                        'm2': hit.source.listing.m2,
                        'price': hit.source.listing.price.amount,
                        'rooms': hit.source.listing.rooms,
                        'bathrooms': hit.source.listing.bathrooms,
                        'location_point': [hit.source.listing.latitude, hit.source.listing.longitude],
                        'images': [image.image for image in hit.source.listing.images],
                        'address': hit.source.listing.address,
                        'neighbourhood': hit.source.listing.locations.location_main.name,
                        'link': 'https://www.fincaraiz.com.co' + hit.source.listing.link,
                    }
                    data.append(extracted_data)
                await rd.set(line[0], json.dumps(data))
        except Exception as error:
                print(error)
        finally:
            await rd.aclose()

scheduler.add_job(update_cache, 'interval', hours=5)
scheduler.start()

class _Shards(BaseModel):
    total: int
    successful: int
    skipped: int
    failed: int

class Total(BaseModel):
    value: int
    relation: str

class Currency(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    rate: Optional[float] = None

class Price(BaseModel):
    amount: Optional[int] = None
    admin_included: Optional[int] = None
    hidePrice: Optional[bool] = None
    currency: Currency

class Subsidiary(BaseModel):
    id: int
    name: str
    masked_phone: str
    address: str
    office_hours: str

class Owner(BaseModel):
    id: int
    name: str
    masked_phone: str
    has_whatsapp: bool
    active: bool
    logo: Optional[str] = None
    inmoLink: Optional[str] = None
    inmoPropsLink: str
    inmofull: bool
    type: Optional[str] = None
    particular: bool
    address: str
    subsidiaries: List[Subsidiary]

class CountryItem(BaseModel):
    id: str
    name: str
    slug: List[str]

class StateItem(BaseModel):
    id: str
    name: str
    slug: List[str]
    indicative: Optional[str] = None

class CityItem(BaseModel):
    id: str
    name: str
    slug: List[str]

class CommuneItem(BaseModel):
    id: str
    name: str
    slug: List[str]

class NeighbourhoodItem(BaseModel):
    id: str
    name: str
    slug: List[str]

class LocationMain(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    slug: Optional[List[str]] = None
    location_type: Optional[str] = None

class Locations(BaseModel):
    location_point: str
    location_main: Optional[LocationMain] = None
    country: List[CountryItem]
    state: List[StateItem]
    city: List[CityItem]
    locality: List
    commune: List[CommuneItem]
    zone: List
    region: List
    neighbourhood: List[NeighbourhoodItem]

class TechnicalSheetItem(BaseModel):
    field: str
    value: Optional[Union[int, str]] = None
    text: str

class PropertyType(BaseModel):
    id: int
    name: str

class OperationType(BaseModel):
    id: int
    name: str

class Facility(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    group: Optional[str] = None

class Image(BaseModel):
    id: int
    image: str
    tag: Optional[str] = None

class CommonExpenses(BaseModel):
    amount: Optional[int] = None
    hidePrice: bool

class SocialMediaLink(BaseModel):
    slug: str
    name: str
    url: str
    icon: str
    order: Optional[str] = None

class Event(BaseModel):
    show: bool
    fecha_inicio_evento: Optional[str] = None
    link: Optional[str] = None
    logo: Optional[str] = None
    tag: Optional[str] = None

class TurboProduct(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    duration: Optional[Union[str, int]] = None

class Listing(BaseModel):
    id: int
    idFincaLegacy: Optional[int] = None
    title: str
    typeID: int
    address: str
    showAddress: bool
    country_id: int
    description: str
    code: str
    code2: Optional[str] = None
    finances: bool
    grouped_ids: Optional[str] = None
    img: Optional[str] = None
    isMapFeatured: bool
    price: Price
    price_amount_usd: Optional[int] = None
    owner: Owner
    locations: Locations
    seller: Optional[str] = None
    technicalSheet: List[TechnicalSheetItem]
    image_count: int
    file_count: int
    imgSize: Optional[int] = None
    link: str
    property_type: PropertyType
    operation_type: OperationType
    barter: bool
    facilities: Optional[List[Facility]] = None
    m2: float
    hectares: Optional[Union[str, float]] = None 
    created_at: str
    updated_at: str
    latitude: float
    longitude: float
    images: Optional[List[Image]] = None
    files: List
    source: Optional[int] = None
    color: Optional[int] = None
    project_group_md5: Optional[str] = None
    project: Optional[List] = None
    pausd: bool
    pointType: bool
    zoom: int
    youtube: str
    highlight: int
    active: bool
    deleted: bool
    relevance: float
    construction_year: Optional[int] = None
    notes: str
    sold: bool
    soldDate: Optional[str] = None
    discount: Optional[str] = None
    draft: bool
    sign: Optional[str] = None
    guarantee: Optional[str] = None
    facilitiesNotApply: bool
    commonExpenses: Optional[CommonExpenses] = None
    price_variation: Dict[str, Any]
    ceCurrencyID: Optional[int] = None
    highlightDate: Optional[str] = None
    socialMediaLinks: List[SocialMediaLink]
    m2Built: Optional[float] = None
    garage: int
    office: bool
    dispositionID: int
    bathrooms: int
    rooms: int
    seaview: bool
    livingPlace: bool
    condominium: bool
    frontLength: Optional[str] = None
    floorsCount: int
    m2apto: float
    apartmentsPerFloor: int
    floor: Optional[int] = None
    neighborhood_id: int
    estate_id: int
    farmhouse: bool
    allowedHeight: str
    isFavorite: bool
    isExternal: bool
    guests: Optional[Union[str, int]] = None
    seasons: List
    occupancies: List
    temporal_price: Optional[str] = None
    temporal_currency: Dict[str, Any]
    seaDistanceName: Optional[str] = None
    tour3d: Optional[str] = None
    legacy_propID: int
    isProject: bool
    operation_type_id: int
    property_type_id: int
    currency: str
    currency_id: int
    hidePrice: bool
    stratum: int
    commonExpenses_currency: Optional[str] = None
    constStatesID: int
    bedrooms: int
    event: Event
    turbo_product: TurboProduct
    tag: Dict[str, Any]
    commercial_units: Optional[Dict[str, Any]] = None
    modify_score: Optional[str] = None
    modify_score_client: int
    description_count: int
    categories_count: int
    has_images: bool
    has_video: bool
    paid_quota: bool
    logo: Optional[str] = None
    include_administration: bool

class _Source(BaseModel):
    listing: Listing

class Fields(BaseModel):
    listing_project_group_md5: List[str] = Field(..., alias='listing.project_group_md5')

class Hit(BaseModel):
    index: Optional[str] = Field(None, alias='_index')
    id: Optional[str] = Field(None, alias='_id')
    score: Optional[float] = Field(None, alias='_score') 
    source: Optional[_Source] = Field(None, alias='_source')
    fields: Optional[Any] = None

class Hits(BaseModel):
    total: Total
    max_score: Optional[float] = None
    hits: List[Hit]

class Cache(BaseModel):
    id: str
    time_type: str
    time_data: int
    datetime: str

class Data(BaseModel):
    cache: Cache

class Model(BaseModel):
    took: int
    timed_out: bool
    _shards: _Shards
    hits: Hits
    data: Data

async def fetch(key: str):
    rd = Redis.from_url(url=os.getenv('REDIS_URL'))
    try:
        yield await rd.get(key)
    except Exception as error:
        print(error)
    finally:
        await rd.aclose()

@router.post('/fr/bello', tags=['bello'])
async def bello():
    return StreamingResponse(fetch(LOCATIONS[0][0]))

@router.post('/fr/medellin', tags=['medellin'])
async def medellin():
    return StreamingResponse(fetch(LOCATIONS[1][0]))

@router.post('/fr/envigado', tags=['envigado'])
async def envigado():
    return StreamingResponse(fetch(LOCATIONS[2][0]))

@router.post('/fr/itagui', tags=['itagui'])
async def itagui():
    return StreamingResponse(fetch(LOCATIONS[3][0]))

@router.post('/fr/sabaneta', tags=['sabaneta'])
async def sabaneta():
    return StreamingResponse(fetch(LOCATIONS[4][0]))

@router.post('/fr/estrella', tags=['estrella'])
async def estrella():
    return StreamingResponse(fetch(LOCATIONS[5][0]))

@router.post('/fr/caldas', tags=['caldas'])
async def caldas():
    return StreamingResponse(fetch(LOCATIONS[6][0]))

@router.post('/fr/copacabana', tags=['copacabana'])
async def copacabana():
    return StreamingResponse(fetch(LOCATIONS[7][0]))

@router.post('/fr/girardota', tags=['girardota'])
async def girardota():
    return StreamingResponse(fetch(LOCATIONS[8][0]))
