#import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes.workbench import WorkbenchAPI
from routes.bot_panel import BotPanelAPI
from fastapi.staticfiles import StaticFiles

class App:
    def __init__(self):
        self.app = FastAPI()
        self.setup_middleware()
        self.app.mount("/public", StaticFiles(directory="public", html=True), name="public")
        self.app.include_router(WorkbenchAPI().router)
        self.app.include_router(BotPanelAPI().router)

    def setup_middleware(self):
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def get_app(self):
        return self.app

app = App().get_app()

if __name__ == "__main__":
    print("\nWorkbench: http://localhost:9000/public")
    #uvicorn.run(App().get_app(), host="0.0.0.0", port=9000)
