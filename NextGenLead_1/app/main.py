import psycopg2
from flask import Flask, request
from flask_restful import Api
from sqlalchemy import Column, String, Integer, Date, BOOLEAN, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
import os

app = Flask(__name__)
api = Api(app)


Base = declarative_base()
database_url = "postgresql://postgres:1234@localhost:5432/postgres"


# disable sqlalchemy pool using NullPool as by default Postgres has its own pool
engine = create_engine(database_url, echo=True, poolclass=NullPool)

conn = engine.connect()

# original table


class ProductEnquiryForms(Base):
    __tablename__ = 'productenquiryforms'
    CustomerName = Column("customername", String)
    Gender = Column("gender", String)
    Age = Column("age", Integer)
    Occupation = Column("occupation", String)
    MobileNo = Column("mobileno", Integer, primary_key=True)
    Email = Column("email", String)
    VehicleModel = Column("vechiclemodel", String)
    State = Column("state", String)
    District = Column("district", String)
    City = Column("city", String)
    ExistingVehicle = Column("existingvehicle", String)
    DealerState = Column("dealerstate", String)
    DealerTown = Column("dealertown", String)
    Dealer = Column("dealer", String)
    BriefAboutEnquiry = Column("briefaboutenquiry", String)
    ExpectedDateOfPurchase = Column("expecteddateofpurchase", Date)
    IntendedUsage = Column("intendedusage", String)
    SentToDealer = Column("senttodealer", BOOLEAN)
    DealerCode = Column("dealercode", String)
    LeadId = Column("leadid", String)
    Comments = Column("comments", String)
    CreatedDate = Column("createddate", Date)
    IsPurchased = Column("ispurchased", BOOLEAN)

#view table
class CustomerDetails(Base):
    __tablename__='customerdetails'
    leadid = Column("leadid", String)
    customername = Column("customername", String)
    mobile = Column("mobileno", Integer, primary_key=True)
    city = Column("city", String)
    dealer = Column("dealer", String)
    dealercode = Column("dealercode", String)
    senttodealer = Column("senttodealer", BOOLEAN)


Session = sessionmaker(bind=engine)
session = Session()


@app.route('/', methods=['GET'])
def home():
    dealercode = request.args.get("dealercode")
    result = session.query(ProductEnquiryForms).filter(ProductEnquiryForms.SentToDealer == 'False',
                                                       ProductEnquiryForms.DealerCode == dealercode).all()
    print(type(result))
    result = [item.__dict__ for item in result]
    mobileno_container = []
    for item in result:
        item.pop('_sa_instance_state')
        mobileno_container.append(item.get('MobileNo'))
    enable_sent_flag(mobileno_container)
    return str(result)


def enable_sent_flag(mobileno_container):
    print("Container {}".format(mobileno_container))
    for mobileno in mobileno_container:
        session.query(ProductEnquiryForms).filter(ProductEnquiryForms.MobileNo == mobileno) \
            .update({"SentToDealer": True})
        session.commit()

@app.route('/senttodealer', methods=['GET'])
def sent_to_dealer():
    dealercode = request.args.get("dealercode")
    # in view table we have rows with all senttodealer flag as false
    # in below query we get data of required dealercode from view table
    result = session.query(CustomerDetails).filter(CustomerDetails.dealercode == dealercode)
    result = [item.__dict__ for item in result]
    print(result)
    # a empty list is created
    customer = []
    for item in result:
        item.pop('_sa_instance_state')
        # list is appended with primary key(mobile number) of all rows queried above which is in dict format
        customer.append(item.get('mobile'))
    sent_flag(customer)
    return str(result)


def sent_flag(customer):
    print("customer {}".format(customer))
    # senttodealer flag is updated as True in original table
    for number in customer:
        session.query(ProductEnquiryForms).filter(ProductEnquiryForms.MobileNo == number).\
            update({"SentToDealer": True})
        session.commit()



@app.route('/get_single_record', methods=['GET'])
def get_single_record():
    leadid = request.args.get("leadid")
    result = session.query(ProductEnquiryForms).filter(ProductEnquiryForms.LeadId == leadid).all()
    result = [item.__dict__ for item in result]
    for item in result:
        item.pop('_sa_instance_state')
    return str(result)


# querying using limit
@app.route('/get_limited_records', methods=['GET'])
def get_limited_records():
    result = session.query(ProductEnquiryForms).limit(os.getenv("LIMIT")).offset(0).all()
    result = [item.__dict__ for item in result]
    for item in result:
        item.pop('_sa_instance_state')
    return str(result)



@app.route('/get_limited_records1', methods=['GET'])
def get_limited_records1():
    # Limit: How many leads to distribute, offset: Stating point
    result = session.query(ProductEnquiryForms).limit(3).offset(2).all()
    result = [item.__dict__ for item in result]
    for item in result:
        item.pop('_sa_instance_state')
    return str(result)



@app.route('/historic_leads', methods=['GET'])
def get_historic_leads():
    start_date = request.args.get("startdate")
    end_date = request.args.get("enddate")
    result = session.query(ProductEnquiryForms).filter(and_(ProductEnquiryForms.CreatedDate >= start_date,
                                                            ProductEnquiryForms.CreatedDate <= end_date)).all()
    result = [item.__dict__ for item in result]
    for item in result:
        item.pop('_sa_instance_state')
    return str(result)



@app.route('/purchased_historic_leads', methods=['GET'])
def get_purchased_leads():
    start_date = request.args.get("startdate")
    end_date = request.args.get("enddate")
    dealer_code = request.args.get("dealercode")
    result = session.query(ProductEnquiryForms).filter(and_(ProductEnquiryForms.CreatedDate >= start_date,
                                                            ProductEnquiryForms.CreatedDate <= end_date,
                                                            ProductEnquiryForms.IsPurchased == 'True',
                                                            ProductEnquiryForms.DealerCode == dealer_code)).all()
    result = [item.__dict__ for item in result]
    for item in result:
        item.pop('_sa_instance_state')
    return str(result)


# method to get not purchased leads for required time period in date format for each dealer code
@app.route('/not_purchased_historic_leads', methods=['GET'])
def get_not_purchased_leads():
    start_date = request.args.get("startdate")
    end_date = request.args.get("enddate")
    dealer_code = request.args.get("dealercode")
    result = session.query(ProductEnquiryForms).filter(and_(ProductEnquiryForms.CreatedDate >= start_date,
                                                            ProductEnquiryForms.CreatedDate <= end_date,
                                                            ProductEnquiryForms.IsPurchased == 'False',
                                                            ProductEnquiryForms.DealerCode == dealer_code)).all()
    result = [item.__dict__ for item in result]
    for item in result:
        item.pop('_sa_instance_state')
    return str(result)


@app.route('/post_records', methods=['POST'])
def home1():
    request_body = request.get_json(force=True)
    for index, item in enumerate(request_body):
        record = ProductEnquiryForms(CustomerName=item["customername"],
                                     Gender=item["gender"],
                                     Age=item["age"],
                                     Occupation=item["occupation"],
                                     MobileNo=item["mobileno"],
                                     Email=item["email"],
                                     VechicleModel=item["vechiclemodel"],
                                     State=item["state"],
                                     District=item["district"],
                                     City=item["city"],
                                     ExistingVehicle=item["existingvehicle"],
                                     DealerState=item["dealerstate"],
                                     DealerTown=item["dealertown"],
                                     Dealer=item["dealer"],
                                     BriefAboutEnquiry=item["briefaboutenquiry"],
                                     ExpectedDateofPurchase=item["expecteddateofpurchase"],
                                     IntendedUsage=item["intendedusage"],
                                     SentToDealer=item["senttodealer"],
                                     DealerCode=item["dealercode"])

        session.add_all([record])
    session.commit()
    return "data inserted"


@app.route('/put_record', methods=['PUT'])
def put_record():
    request_body = request.get_json(force=True)
    try:
        result = session.query(ProductEnquiryForms). \
            filter(ProductEnquiryForms.MobileNo == ProductEnquiryForms(request_body[0]["mobileno"])) \
            .update(record=ProductEnquiryForms(request_body[0]["comments"]))
        session.commit()
        return str(result)
    finally:
        session.close()


@app.route('/patch_record', methods=['PATCH'])
def patch_record():
    print("parameter is {}".format(request.args))
    cust_name = request.args.get("customername")
    try:
        result = session.query(ProductEnquiryForms).filter(ProductEnquiryForms.CustomerName == cust_name) \
            .update({"DealerCode": 'bng002'})
        session.commit()
        return str(result)
    finally:
        session.close()


@app.route('/del_single_record', methods=['DELETE'])
def del_record():
    from flask import request
    print("parameter is {}".format(request.args))
    date = request.args.get("expecteddateofpurchase")
    try:
        result = session.query(ProductEnquiryForms).filter(ProductEnquiryForms.ExpectedDateOfPurchase < date).delete()
        session.commit()
        return str(result)
    finally:
        pass


if __name__ == "__main__":
    app.run(debug=True)