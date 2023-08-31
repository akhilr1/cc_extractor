from fastapi import FastAPI
from pydantic import BaseModel
# from .config import settings
from .cc_engine import cc_worker





app = FastAPI()

class cc_input(BaseModel):
    domain: str
    year: int


@app.get("/")
def root():
    return {"message": "Hello"}

@app.post("/domain")
def extract_by_domain(input: cc_input):
    input_dict = input.model_dump()
    year = input_dict['year']
    domain = input_dict['domain']
    res=cc_worker(domain,year)
    return res
   
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)