from fastapi import FastAPI
from pydantic import BaseModel
from data_loader import load_data
from tools import count_by_specialty, count_by_city, filter_active, plot_top_specialties

app = FastAPI(title="NPPES Agent API")
df = load_data()


class ChatRequest(BaseModel):
    message: str


@app.get("/")
def root():
    return {"message": "NPPES Agent is running"}


@app.get("/summary")
def summary():
    return {
        "rows": int(len(df)),
        "columns": list(df.columns)
    }


@app.get("/specialties")
def specialties():
    return count_by_specialty(df)[:10]


@app.get("/cities")
def cities():
    return count_by_city(df)[:10]


@app.get("/active")
def active():
    return filter_active(df)


@app.get("/plot-specialties")
def plot_specialties(top_n: int = 10):
    return plot_top_specialties(df, top_n=top_n)


@app.post("/chat")
def chat(req: ChatRequest):
    msg = req.message.lower().strip()

    if "specialt" in msg and "plot" in msg:
        result = plot_top_specialties(df, top_n=10)
        return {
            "answer": "I created a plot of the top 10 specialties.",
            "plot": result["image_path"],
            "data": result["top_specialties"]
        }

    if "specialt" in msg:
        result = count_by_specialty(df)[:10]
        return {
            "answer": "Here are the top specialties.",
            "data": result
        }

    if "city" in msg:
        result = count_by_city(df)[:10]
        return {
            "answer": "Here are the top cities.",
            "data": result
        }

    if "active" in msg:
        result = filter_active(df)
        return {
            "answer": "Here is the active-record summary.",
            "data": result
        }

    return {
        "answer": "I can help with specialties, cities, active records, and specialty plots."
    }
